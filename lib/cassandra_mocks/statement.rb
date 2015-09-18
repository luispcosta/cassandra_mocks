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
        if type.insert?
          3.times { tokens.pop }

          insert_keys = []
          until (key = tokens.pop).rparen?
            insert_keys << key.value unless key.comma?
          end

          2.times { tokens.pop }
          insert_values = []
          until (key = tokens.pop).rparen?
            insert_values << key.value unless key.comma?
          end

          @args = insert_args(insert_keys, insert_values, args)
        end
      end

      private

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
