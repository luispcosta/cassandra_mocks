module Cassandra
  module Mocks
    class Session
      attr_reader :keyspace, :cluster

      def initialize(keyspace, cluster)
        @keyspace = keyspace
        @cluster = cluster
      end

      def close_async
        Cassandra::Future.value(nil)
      end

      def close
        close_async.get
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
              cluster.add_keyspace(statement.args[:keyspace])
            when :create_table
              cluster.keyspace(keyspace).add_table(statement.args[:table], statement.args[:primary_key], statement.args[:columns])
            when :insert
              cluster.keyspace(statement.args[:keyspace] || keyspace).table(statement.args[:table]).insert(statement.args[:values])
            when :update
              table = cluster.keyspace(statement.args[:keyspace] || keyspace).table(statement.args[:table])
              row = table.select('*', statement.args[:filter]).first.merge('field1' => statement.args[:values]['field1'])
              cluster.keyspace(statement.args[:keyspace] || keyspace).table(statement.args[:table]).insert(row)
            when :truncate
              cluster.keyspace(statement.args[:keyspace] || keyspace).table(statement.args[:table]).rows.clear
            when :drop_keyspace
              cluster.drop_keyspace(statement.args[:keyspace])
            when :drop_table
              cluster.keyspace(statement.args[:keyspace] || keyspace).drop_table(statement.args[:table])
            when :select
              table = cluster.keyspace(statement.args[:keyspace] || keyspace).table(statement.args[:table])
              table.select(*statement.args[:columns], statement.args[:filter].merge(limit: statement.args[:limit]))
          end
        end
      end

      def execute(cql)
        execute_async(cql).get
      end

      def ==(rhs)
        rhs.is_a?(Session) &&
            rhs.keyspace == keyspace &&
            rhs.cluster == cluster
      end

    end
  end
end
