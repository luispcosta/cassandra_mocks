module Cassandra
  module Mocks
    class Session
      def prepare_async(cql)
        Cassandra::Future.value(Statement.new(cql, []))
      end
    end
  end
end
