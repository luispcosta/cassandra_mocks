module Cassandra
  module Mocks
    class Session
      attr_reader :keyspace

      def initialize(keyspace, cluster)
        @keyspace = keyspace
        @cluster = cluster
      end

      def prepare_async(cql)
        Cassandra::Future.value(Statement.new(cql, []))
      end

      def prepare(cql)
        prepare_async(cql).get
      end

      def execute_async(cql)
        prepare_async(cql).then do |statement|
          if statement.action == :create_keyspace
            @cluster.add_keyspace(statement.args[:keyspace])
          else
            @cluster.keyspace(keyspace).add_table(statement.args[:table], statement.args[:primary_key], statement.args[:columns])
          end
        end
      end

      def execute(cql)
        execute_async(cql).get
      end

    end
  end
end
