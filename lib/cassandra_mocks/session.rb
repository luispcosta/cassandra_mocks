module Cassandra
  module Mocks
    class Session
      attr_reader :keyspace

      def initialize(keyspace)
        @keyspace = keyspace
      end

      def prepare_async(cql)
        Cassandra::Future.value(Statement.new(cql, []))
      end

      def prepare(cql)
        prepare_async(cql).get
      end

    end
  end
end
