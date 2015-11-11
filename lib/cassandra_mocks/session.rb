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
          result = []
          case statement.action
            when :create_keyspace
              cluster.add_keyspace(statement.args[:keyspace])
            when :create_table
              cluster.keyspace(keyspace).add_table(statement.args[:table], statement.args[:primary_key], statement.args[:columns])
            when :insert
              insert_query(result, statement)
            when :update
              update_query(statement)
            when :truncate
              cluster.keyspace(keyspace_for_statement(statement)).table(statement.args[:table]).rows.clear
            when :drop_keyspace
              cluster.drop_keyspace(statement.args[:keyspace])
            when :drop_table
              cluster.keyspace(keyspace_for_statement(statement)).drop_table(statement.args[:table])
            when :select
              result = select_query(statement)
            when :delete
              delete_query(statement)
          end
          result
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

      private

      def insert_query(result, statement)
        check_exists = !!statement.args[:check_exists]
        inserted = cluster.keyspace(keyspace_for_statement(statement)).table(statement.args[:table]).insert(statement.args[:values], check_exists: check_exists)
        result << {'[applied]' => inserted} if check_exists
      end

      def select_query(statement)
        table = cluster.keyspace(keyspace_for_statement(statement)).table(statement.args[:table])
        options = statement.args[:filter].merge(limit: statement.args[:limit], order: statement.args[:order])
        table.select(*statement.args[:columns], options)
      end

      def delete_query(statement)
        table = cluster.keyspace(keyspace_for_statement(statement)).table(statement.args[:table])
        table.delete(statement.args[:filter])
      end

      def update_query(statement)
        table = cluster.keyspace(keyspace_for_statement(statement)).table(statement.args[:table])
        rows_to_update = table.select('*', statement.args[:filter])
        rows_to_update = [statement.args[:filter].dup] if rows_to_update.empty?
        rows_to_update.each do |row|
          updated_row = updated_row(row, statement)
          cluster.keyspace(keyspace_for_statement(statement)).table(statement.args[:table]).insert(updated_row)
        end
      end

      def updated_row(row, statement)
        statement.args[:values].inject(row.dup) do |memo, (column, value)|
          if value.is_a?(Statement::Arithmetic)
            value.apply(memo)
          else
            memo.merge!(column => value)
          end
        end
      end

      def keyspace_for_statement(statement)
        statement.args[:keyspace] || keyspace
      end

    end
  end
end
