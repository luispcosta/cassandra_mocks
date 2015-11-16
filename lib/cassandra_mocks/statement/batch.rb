module Cassandra
  module Statements
    class Batch
      BatchedStatement = Struct.new(:statement, :args)

      def add(statement, *args)
        @statements << BatchedStatement.new(statement, args)
      end

    end
  end
end
