module Cassandra
  module Mocks
    class Statement
      attr_reader :cql, :action, :args

      def initialize(cql, args)
        @cql = cql

        type_token = next_token
        @action = type_token.type
        if type_token.create?
          create_type_token = next_token
          if create_type_token.table?
            @action = :create_table
            parse_create_table
          else
            @action = :create_keyspace
            @args = {keyspace: next_token.value}
          end
        elsif type_token.insert?
          parse_insert_query(args)
        elsif type_token.select?
          parse_select_query(args)
        elsif type_token.delete?
          next_token
          parse_table_and_filter(args)
        end
      end

      def fill_params(params)
        Statement.allocate.tap do |statement|
          statement.cql = cql
          statement.action = action
          params = param_queue(params)
          filter = args[:filter].inject({}) do |memo, (column, value)|
            memo.merge!(column => (value || params.pop))
          end
          statement.args = args.merge(filter: filter)
        end
      end

      def ==(rhs)
        rhs.is_a?(Statement) &&
            rhs.action == action &&
            rhs.args == args
      end

      protected

      attr_writer :cql, :action, :args

      private

      def tokens
        @tokens ||= Tokenizer.new(@cql).token_queue
      end

      def next_token
        if tokens.empty?
          Token.new(:eof, nil)
        else
          tokens.pop
        end
      end

      def parse_create_table
        table_name = next_token.value

        next_token
        column_name = next_token.value
        column_type = next_token.value
        primary_key = nil
        if next_token.primary?
          primary_key = [[column_name]]
          2.times { next_token }
        end

        additional_columns = if tokens.empty?
                               {}
                             else
                               parenthesis_values(:rparen, :primary).each_slice(2).inject({}) do |memo, (name, type)|
                                 memo.merge!(name => type)
                               end
                             end

        if !tokens.empty? && next_token.key?
          next_token
          primary_key_parts = parenthesis_values(:rparen)
          partition_key = primary_key_parts.shift
          partition_key = [partition_key] unless partition_key.is_a?(Array)
          primary_key = [partition_key, *primary_key_parts]
        end

        @args = {table: table_name, columns: additional_columns.merge({column_name => column_type}), primary_key: primary_key}
      end

      def parse_insert_query(args)
        keyspace_name = nil

        next_token
        table_name = next_token.value
        if next_token.dot?
          keyspace_name = table_name
          table_name = next_token.value
          next_token
        end

        insert_keys = parenthesis_values(:rparen)
        2.times { next_token }
        insert_values = parenthesis_values(:rparen)

        values = insert_args(insert_keys, insert_values, args)
        @args = {keyspace: keyspace_name, table: table_name, values: values}
      end

      def parse_select_query(args)
        select_columns = parenthesis_values(:from)
        parse_table_and_filter(args)
        @args.merge!(columns: select_columns)
      end

      def parse_table_and_filter(args)
        keyspace_name = nil

        table_name = next_token.value
        if (token = next_token) && token.dot?
          keyspace_name = table_name
          table_name = next_token.value
        end

        filter_keys = []
        filter_values = []
        until tokens.empty?
          filter_keys << next_token.value
          next_token
          value_token = next_token
          if value_token.type == :in
            next_token
            filter_values << parenthesis_values(:rparen)
          else
            filter_values << value_token.normalized_value
          end
          next_token unless tokens.empty?
        end
        filter = insert_args(filter_keys, filter_values, args)

        @args = {keyspace: keyspace_name, table: table_name, filter: filter}
      end

      def parenthesis_values(*terminators)
        [].tap do |insert_values|
          until terminators.include?((key = next_token).type)
            if key.lparen?
              insert_values << parenthesis_values(:rparen)
            elsif !key.comma?
              insert_values << key.normalized_value unless key.comma?
            end
          end
        end
      end

      def insert_args(insert_keys, insert_values, args)
        args = param_queue(args)
        insert_keys.count.times.inject({}) do |memo, index|
          value = mapped_value(args, insert_values[index])
          memo.merge!(insert_keys[index] => value)
        end
      end

      def mapped_value(args, value)
        if value.is_a?(Array)
          value.map { |value| parameterized_value(args, value) }
        else
          parameterized_value(args, value)
        end
      end

      def parameterized_value(args, value)
        if value == '?'
          args.pop unless args.empty?
        else
          value
        end
      end

      def param_queue(args)
        Queue.new.tap { |queue| args.each { |arg| queue << arg } }
      end
    end
  end
end
