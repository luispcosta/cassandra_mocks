module Cassandra
  module Mocks
    class Keyspace < ::Cassandra::Keyspace

      def initialize(name)
        replication = Cassandra::Keyspace::Replication.new('mock', {})
        super(name, false, replication, [])
      end

    end
  end
end