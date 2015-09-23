module Cassandra
  module Mocks
    class Statement
      class Arithmetic < Struct.new(:operation, :column, :amount)

        def apply(row)
          row.merge(column => row[column].public_send(operator, amount))
        end

        private

        def operator
          operation == :plus ? :+ : :-
        end

      end
    end
  end
end
