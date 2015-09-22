module Cassandra
  module Mocks
    class Cluster
      extend Forwardable

      def_delegator :@keyspaces, :[], :keyspace

      def initialize
        @keyspaces = {}
      end

      def add_keyspace(name)
        @keyspaces[name] = Keyspace.new(name)
      end

      def each_keyspace(&block)
        @keyspaces.values.each(&block)
      end

    end
  end
end
