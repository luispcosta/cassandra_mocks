module Cassandra
  module Mocks
    class Statement
      attr_reader :cql, :action, :args

      PARENS_MATCHER = /\(\s*([^(]+)\s*\)/

      def initialize(cql, args)
        @cql = cql
        @action = :insert

        tokens = Tokenizer.new(@cql).token_queue
        type = tokens.pop
        parse_insert_query(args, tokens) if type.insert?
      end

      private

      def parse_insert_query(args, tokens)
        3.times { tokens.pop }

        insert_keys = parenthesis_values(tokens)
        2.times { tokens.pop }
        insert_values = parenthesis_values(tokens)

        @args = insert_args(insert_keys, insert_values, args)
      end

      def parenthesis_values(tokens)
        [].tap do |insert_values|
          until (key = tokens.pop).rparen?
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
