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

    end
  end
end
