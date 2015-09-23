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

      def execute_async(cql, *args)
        future = cql.is_a?(Statement) ? Cassandra::Future.value(cql.fill_params(args)) : prepare_async(cql)
        future.then do |statement|
          case statement.action
            when :create_keyspace
              @cluster.add_keyspace(statement.args[:keyspace])
            when :create_table
              @cluster.keyspace(keyspace).add_table(statement.args[:table], statement.args[:primary_key], statement.args[:columns])
            when :insert
              @cluster.keyspace(statement.args[:keyspace] || keyspace).table(statement.args[:table]).insert(statement.args[:values])
            when :truncate
              @cluster.keyspace(statement.args[:keyspace] || keyspace).table(statement.args[:table]).rows.clear
            when :select
              table = @cluster.keyspace(statement.args[:keyspace] || keyspace).table(statement.args[:table])
              if statement.args[:filter].empty?
                table.select(*statement.args[:columns])
              else
                table.select(*statement.args[:columns], statement.args[:filter])
              end
          end
        end
      end

      def execute(cql)
        execute_async(cql).get
      end

    end
  end
end
