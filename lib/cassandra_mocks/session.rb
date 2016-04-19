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
        options = fetch_options(args)
        args = options.fetch(:arguments) { args }

        if cql.is_a?(Cassandra::Statements::Batch)
          futures = cql.statements.map do |batched_statement|
            execute_async(batched_statement.statement, *batched_statement.args)
          end
          return Cassandra::Future.all(futures).then { ResultPage.new }
        end

        future = cql.is_a?(Statement) ? Cassandra::Future.value(cql.fill_params(args)) : prepare_async(cql)
        future.then do |statement|
          result = ResultPage.new
          # noinspection RubyCaseWithoutElseBlockInspection
          case statement.action
            when :create_keyspace
              cluster.add_keyspace(statement.args[:keyspace], statement.args[:check_exists])
            when :create_table
              cluster.keyspace(keyspace).add_table(statement.args[:table], statement.args[:primary_key], statement.args[:columns], statement.args[:check_exists])
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

      def fetch_options(args)
        if args.last.is_a?(Hash)
          args.pop
        else
          {arguments: args}
        end
      end

      def insert_query(result, statement)
        check_exists = !!statement.args[:check_exists]
        table = cluster.keyspace(keyspace_for_statement(statement)).table(statement.args[:table])
        raise Cassandra::Errors::InvalidError.new('INSERT statement are not allowed on counter tables, use UPDATE instead', 'MockStatement') if table.counter_table?
        inserted = table.insert(statement.args[:values], check_exists: check_exists)
        result << {'[applied]' => inserted} if check_exists
      end

      def select_query(statement)
        table = cluster.keyspace(keyspace_for_statement(statement)).table(statement.args[:table])
        options = {restriction: statement.args[:filter], limit: statement.args[:limit], order: statement.args[:order]}
        table.select(*statement.args[:columns], options)
      end

      def delete_query(statement)
        table = cluster.keyspace(keyspace_for_statement(statement)).table(statement.args[:table])
        table.delete(statement.args[:filter])
      end

      def update_query(statement)
        table = cluster.keyspace(keyspace_for_statement(statement)).table(statement.args[:table])
        rows_to_update = table.select('*', restriction: statement.args[:filter])
        rows_to_update = [statement.args[:filter].dup] if rows_to_update.empty?
        rows_to_update.each do |row|
          updated_row = updated_row(table, row, statement)
          cluster.keyspace(keyspace_for_statement(statement)).table(statement.args[:table]).insert(updated_row)
        end
      end

      def updated_row(table, row, statement)
        statement.args[:values].inject(row.dup) do |memo, (column, value)|
          if value.is_a?(Statement::Arithmetic)
            raise Cassandra::Errors::InvalidError.new("Invalid operation (#{column} = #{column} + ?) for non counter column #{column}", 'MockStatement') unless table.counter_table?
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
