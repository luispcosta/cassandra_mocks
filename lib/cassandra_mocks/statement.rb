module Cassandra
  module Mocks
    class Statement
      attr_reader :cql, :action, :args

      PARENS_MATCHER = /\(\s*([^(]+)\s*\)/

      def initialize(cql, args)
        @cql = cql

        tokens = Tokenizer.new(@cql).token_queue
        type = tokens.pop
        if type.insert?
          @action = :insert
          parse_insert_query(args, tokens)
        elsif type.select?
          @action = :select
          key_space = nil

          select_columns = parenthesis_values(tokens, :from)
          table_name = tokens.pop.value
          if !tokens.empty? && tokens.pop.dot?
            keyspace_name = table_name
            table_name = tokens.pop.value
          end

          @args = {
              keyspace: keyspace_name,
              table: table_name,
              columns: select_columns,
              filter: {}
          }
        end
      end

      private

      def parse_insert_query(args, tokens)
        keyspace_name = nil

        tokens.pop
        table_name = tokens.pop.value
        if tokens.pop.dot?
          keyspace_name = table_name
          table_name = tokens.pop.value
          tokens.pop
        end

        insert_keys = parenthesis_values(tokens, :rparen)
        2.times { tokens.pop }
        insert_values = parenthesis_values(tokens, :rparen)

        values = insert_args(insert_keys, insert_values, args)
        @args = {keyspace: keyspace_name, table: table_name, values: values}
      end

      def parenthesis_values(tokens, terminator)
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
          value = (value == '?') ? args[param_index+=1] : value
          memo.merge!(insert_keys[index] => value)
        end
      end
    end
  end
end
