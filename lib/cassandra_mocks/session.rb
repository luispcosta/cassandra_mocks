module Cassandra
  module Mocks
    class Session
      def prepare_async(cql)
        Cassandra::Future.value(Statement.new(cql, []))
      end

      def prepare(cql)
        prepare_async(cql).get
      end
    end
  end
end
