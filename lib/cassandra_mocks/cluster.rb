module Cassandra
  module Mocks
    class Cluster
      extend Forwardable

      def_delegator :@keyspaces, :[], :keyspace

      def initialize
        @keyspaces = {}
      end

      def connect_async(keyspace = nil)
        session = Session.new(keyspace, self)
        Cassandra::Future.value(session)
      end

      def connect(keyspace = nil)
        connect_async(keyspace).get
      end

      def add_keyspace(name)
        @keyspaces[name] = Keyspace.new(name)
      end

      def drop_keyspace(name)
        @keyspaces.delete(name)
      end

      def each_keyspace(&block)
        @keyspaces.values.each(&block)
      end

    end
  end
end
