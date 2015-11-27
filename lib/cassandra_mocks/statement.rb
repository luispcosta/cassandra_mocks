module Cassandra
  module Mocks
    class Statement
      attr_reader :cql, :action, :args

      def initialize(cql, args)
        @cql = cql
        @input_args = param_queue(args)

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
        elsif type_token.truncate?
          parse_truncate_query
        elsif type_token.drop?
          if next_token.keyspace?
            @action = :drop_keyspace
            @args = {keyspace: next_token.value}
          else
            @action = :drop_table
            parse_truncate_query
          end
        elsif type_token.insert?
          parse_insert_query
        elsif type_token.update?
          parse_update_query
        elsif type_token.select?
          parse_select_query
        elsif type_token.delete?
          next_token
          parse_table_and_filter
        end
      end

      def fill_params(params)
        Statement.allocate.tap do |statement|
          statement.cql = cql
          statement.action = action
          statement.args = args.dup
          params = param_queue(params)
          case action
            when :insert
              parameterize_args!(:values, params, statement)
            when :select, :delete
              parameterize_args!(:filter, params, statement)
            when :update
              parameterize_args!(:values, params, statement)
              parameterize_args!(:filter, params, statement)
          end
        end
      end

      def bind(*args)
        fill_params(args)
      end

      def ==(rhs)
        rhs.is_a?(Statement) &&
            rhs.action == action &&
            rhs.args == args
      end

      protected

      attr_writer :cql, :action, :args

      private

      attr_reader :last_token

      def parameterize_args!(key, params, statement)
        values = args[key].inject({}) do |memo, (column, value)|
          updated_value = if value.is_a?(Arithmetic)
                            Arithmetic.new(value.operation, value.column, pending_value(value.amount, params))
                          elsif value.is_a?(Comparitor)
                            Comparitor.new(value.operation, value.column, pending_value(value.value, params))
                          elsif value.is_a?(Array)
                            value.map { |value| pending_value(value, params) }
                          else
                            pending_value(value, params)
                          end
          memo.merge!(column => updated_value)
        end
        statement.args.merge!(key => values)
      end

      def pending_value(value, params)
        if value == :value_pending
          raise Errors::InvalidError.new('Not enough params provided to #fill_params', 'MockStatement') if params.empty?
          params.pop
        else
          value
        end
      end

      def tokens
        @tokens ||= Tokenizer.new(@cql).token_queue
      end

      def next_token
        if tokens.empty?
          Token.new(:eof, nil)
        else
          @last_token = tokens.pop
        end
      end

      def parse_create_table
        table_name_token = next_token
        check_exists = if table_name_token.if?
                         next_token
                         next_token
                         table_name_token = next_token
                       end
        table_name = table_name_token.value

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

        @args = {table: table_name, check_exists: !!check_exists, columns: additional_columns.merge({column_name => column_type}), primary_key: primary_key}
      end

      def parse_truncate_query
        keyspace_name, table_name = parsed_keyspace_and_table

        @args = {keyspace: keyspace_name, table: table_name}
      end

      def parse_insert_query
        next_token

        keyspace_name, table_name = parsed_keyspace_and_table
        next_token unless keyspace_name.nil?

        insert_keys = parenthesis_values(:rparen)
        2.times { next_token }
        insert_values = parenthesis_values(:rparen)

        values = insert_args(insert_keys, insert_values)
        if next_token.if?
          @args = {keyspace: keyspace_name, table: table_name, values: values, check_exists: true}
        else
          @args = {keyspace: keyspace_name, table: table_name, values: values}
        end
      end

      def parse_update_query
        keyspace_name, table_name = parsed_keyspace_and_table
        next_token if keyspace_name
        values = parsed_filter(:where)
        filter = parsed_filter(:eof)
        @args = {keyspace: keyspace_name, table: table_name, values: values, filter: filter}
      end

      def parse_select_query
        select_columns = parenthesis_values(:from)
        parse_table_and_filter
        @args.merge!(columns: select_columns)
      end

      def parse_table_and_filter
        keyspace_name, table_name = parsed_keyspace_and_table
        next_token if keyspace_name

        filter = parsed_filter(:limit, :order)

        @args = {keyspace: keyspace_name, table: table_name, filter: filter}

        if last_token.order?
          next_token
          @args.merge!(order: parse_select_order)
        end

        if last_token.limit?
          limit = next_token.normalized_value
          @args = @args.merge!(limit: limit)
        end
      end

      def parse_select_order
        order = {}
        prev_column = nil
        token = next_token
        until token.eof? || token.limit?
          if token.desc?
            order[prev_column] = :desc
          elsif token.asc?
            order[prev_column] = :asc
          elsif token.comma?
          else
            order[token.value] = :asc
            prev_column = token.value
          end
          token = next_token
        end
        order
      end

      def parsed_filter(*end_tokens)
        filter_keys = []
        filter_values = []
        until tokens.empty? || end_tokens.include?(last_token.type)
          next_key = next_token
          filter_keys << if next_key.lparen?
                           parenthesis_values(:rparen)
                         else
                           next_key.value
                         end

          restrictor_token = next_token
          if restrictor_token.type == :in
            next_token
            filter_values << parenthesis_values(:rparen)
            next_token
          elsif restrictor_token.ltri? || restrictor_token.rtri?
            parse_comparison_restriction(filter_keys, filter_values, restrictor_token)
          else
            parse_single_restriction(filter_values)
          end
        end

        insert_args(filter_keys, filter_values)
      end

      def parse_comparison_restriction(filter_keys, filter_values, restrictor_token)
        value_token = next_token
        eql_comparison = if value_token.type == :eql
                           value_token = next_token
                           true
                         end

        value = if value_token.lparen?
                  parenthesis_values(:rparen)
                else
                  value_token.normalized_value
                end

        comparison_operator = comparison_operator(eql_comparison, restrictor_token)
        filter_values << Comparitor.new(comparison_operator, filter_keys.last, value)
        next_token
      end

      def parse_single_restriction(filter_values)
        value_token = next_token
        next_token
        value = value_token.normalized_value
        update_value = update_value(last_token, value)
        if update_value
          value = update_value
          next_token
        end
        filter_values << value
      end

      def comparison_operator(eql_comparison, restrictor_token)
        if eql_comparison
          restrictor_token.rtri? ? :ge : :le
        else
          restrictor_token.rtri? ? :gt : :lt
        end
      end

      def update_value(prev_token, value)
        if prev_token.plus? || prev_token.minus?
          column = value
          amount = next_token.normalized_value
          Arithmetic.new(prev_token.type, column, amount)
        end
      end

      def parsed_keyspace_and_table
        keyspace_name = nil
        table_name = next_token.value
        if next_token.dot?
          keyspace_name = table_name
          table_name = next_token.value
        end
        [keyspace_name, table_name]
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

      def insert_args(insert_keys, insert_values)
        insert_keys.count.times.inject({}) do |memo, index|
          value = mapped_value(insert_values[index])
          memo.merge!(insert_keys[index] => value)
        end
      end

      def mapped_value(value)
        if value.is_a?(Array)
          value.map { |value| parameterized_value(value) }
        elsif value.is_a?(Arithmetic)
          updated_amount = parameterized_value(value.amount)
          Arithmetic.new(value.operation, value.column, updated_amount)
        elsif value.is_a?(Comparitor)
          updated_amount = parameterized_value(value.value)
          Comparitor.new(value.operation, value.column, updated_amount)
        else
          parameterized_value(value)
        end
      end

      def parameterized_value(value)
        if value == '?'
          if @input_args.empty?
            :value_pending
          else
            @input_args.pop
          end
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
