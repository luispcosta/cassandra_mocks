module Cassandra
  module Mocks
    class Statement
      attr_reader :cql, :action, :args

      def initialize(cql, args)
        @cql = cql

        type_token = tokens.pop
        @action = type_token.type
        if type_token.insert?
          parse_insert_query(args)
        elsif type_token.select?
          parse_select_query(args)
        end
      end

      private

      def tokens
        @tokens ||= Tokenizer.new(@cql).token_queue
      end

      def parse_insert_query(args)
        keyspace_name = nil

        tokens.pop
        table_name = tokens.pop.value
        if tokens.pop.dot?
          keyspace_name = table_name
          table_name = tokens.pop.value
          tokens.pop
        end

        insert_keys = parenthesis_values(:rparen)
        2.times { tokens.pop }
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

        table_name = tokens.pop.value
        if !tokens.empty? && tokens.pop.dot?
          keyspace_name = table_name
          table_name = tokens.pop.value
        end

        filter_keys = []
        filter_values = []
        until tokens.empty?
          filter_keys << tokens.pop.value
          tokens.pop
          value_token = tokens.pop
          if value_token.type == :in
            tokens.pop
            filter_values << parenthesis_values(:rparen)
          else
            filter_values << value_token.value
          end
          tokens.pop unless tokens.empty?
        end
        filter = insert_args(filter_keys, filter_values, args)

        @args = {keyspace: keyspace_name, table: table_name, filter: filter}
      end

      def parenthesis_values(terminator)
        [].tap do |insert_values|
          until (key = tokens.pop).type == terminator
            insert_values << key.value unless key.comma?
          end
        end
      end

      def insert_args(insert_keys, insert_values, args)
        param_index = -1
        insert_keys.count.times.inject({}) do |memo, index|
          value = insert_values[index]
          if value.is_a?(Array)
            value = value.map do |value|
              (value == '?') ? args[param_index+=1] : value
            end
          elsif value == '?'
            value = args[param_index+=1]
          end
          memo.merge!(insert_keys[index] => value)
        end
      end
    end
  end
end
