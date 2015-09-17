module Cassandra
  module Mocks
    class Statement
      class Tokenizer

        attr_reader :tokens

        def initialize(cql)
          @tokens = [:"#{cql.downcase}" => cql]
        end

      end
    end
  end
end
