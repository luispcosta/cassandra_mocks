module Cassandra
  module Mocks
    class Session
      attr_reader :keyspaces

      def initialize
        @keyspaces = []
      end

      def add_keyspace(name)
        keyspaces << Keyspace.new(name)
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
