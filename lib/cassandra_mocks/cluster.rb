module Cassandra
  module Mocks
    class Cluster

      attr_reader :keyspaces

      def initialize
        @keyspaces = []
      end

      def add_keyspace(name)
        keyspaces << Keyspace.new(name)
      end

    end
  end
end
