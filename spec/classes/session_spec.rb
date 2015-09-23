require 'rspec'

module Cassandra
  module Mocks
    describe Session do
      let(:keyspace) { nil }
      let(:cluster) { Cluster.new }

      subject { Session.new(keyspace, cluster) }

      describe '#keyspace' do
        its(:keyspace) { is_expected.to be_nil }

        context 'when a keyspace is specified' do
          let(:keyspace) { 'keyspace' }
          its(:keyspace) { is_expected.to eq('keyspace') }

          context 'with a different keyspace' do
            let(:keyspace) { 'staging' }
            its(:keyspace) { is_expected.to eq('staging') }
          end
        end
      end

      describe '#prepare_async' do
        let(:query) { 'SELECT * FROM everything' }

        it 'should return a Cassandra::Future' do
          expect(subject.prepare_async(query)).to be_a_kind_of(Cassandra::Future)
        end

        it 'should create a statement from the input query' do
          expect(subject.prepare_async(query).get).to eq(Statement.new('SELECT * FROM everything', []))
        end

        context 'with a different query' do
          let(:query) { 'INSERT INTO table (pk1, ck1) VALUES (35, 22)' }

          it 'should create a statement from the input query' do
            expect(subject.prepare_async(query).get).to eq(Statement.new('INSERT INTO table (pk1, ck1) VALUES (35, 22)', []))
          end
        end
      end

      describe '#prepare' do
        let(:query) { 'SELECT * FROM everything' }

        it 'should create a statement from the input query' do
          expect(subject.prepare(query)).to eq(Statement.new('SELECT * FROM everything', []))
        end

        context 'with a different query' do
          let(:query) { 'INSERT INTO table (pk1, ck1) VALUES (35, 22)' }

          it 'should create a statement from the input query' do
            expect(subject.prepare(query)).to eq(Statement.new('INSERT INTO table (pk1, ck1) VALUES (35, 22)', []))
          end
        end
      end

      describe '#execute_async' do
        let(:query) { 'SELECT * FROM everything' }

        it 'should return a Cassandra::Future' do
          expect(subject.execute_async(query)).to be_a_kind_of(Cassandra::Future)
        end

        describe 'with a CREATE KEYSPACE query' do
          let(:query) { "CREATE KEYSPACE keyspace_name WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }" }

          it 'should create the keyspace' do
            subject.execute_async(query).get
            expect(cluster.keyspace('keyspace_name')).to eq(Keyspace.new('keyspace_name'))
          end

          context 'with a different keyspace' do
            let(:query) { "CREATE KEYSPACE development WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }" }

            it 'should create the keyspace' do
              subject.execute_async(query).get
              expect(cluster.keyspace('development')).to eq(Keyspace.new('development'))
            end
          end
        end

        describe 'with a CREATE TABLE query' do
          let(:keyspace) { 'keyspace' }
          let(:table_name) { 'table_name' }
          let(:query) { "CREATE TABLE #{table_name} (pk1 text, ck1 text, field1 text, PRIMARY KEY(pk1, ck1))" }
          let(:table_name) { 'table_name' }
          let(:partition_key) { {'pk1' => 'text'} }
          let(:clustering_columns) { {'ck1' => 'text'} }
          let(:fields) { {'field1' => 'text'} }
          let(:expected_table) do
            Table.new(keyspace, table_name, create_columns(partition_key), create_columns(clustering_columns), create_columns(fields))
          end

          before { cluster.add_keyspace(keyspace) }

          it 'should create the table with the specified params' do
            subject.execute_async(query).get
            table = cluster.keyspace(keyspace).table('table_name')
            expect(table).to eq(expected_table)
          end

          context 'with a different keyspace' do
            let(:keyspace) { 'development' }

            it 'should create the table with the specified params' do
              subject.execute_async(query).get
              table = cluster.keyspace(keyspace).table('table_name')
              expect(table.keyspace).to eq(keyspace)
            end
          end

          context 'with a different table name' do
            let(:table_name) { 'books' }

            it 'should create the table with the specified table name' do
              subject.execute_async(query).get
              table = cluster.keyspace(keyspace).table('books')
              expect(table.name).to eq(table_name)
            end
          end

          context 'with a different partition key' do
            let(:query) { 'CREATE TABLE table_name (section text, region_number int, location text, PRIMARY KEY((section, region_number), location))' }

            it 'should create the table with the specified partition key' do
              subject.execute_async(query).get
              table = cluster.keyspace(keyspace).table('table_name')
              expect(table.partition_key.map(&:name)).to eq(%w(section region_number))
            end
          end

          context 'with different clustering columns' do
            let(:query) { 'CREATE TABLE table_name (section text, region_number int, location text, PRIMARY KEY(section, region_number, location))' }

            it 'should create the table with the specified clustering columns' do
              subject.execute_async(query).get
              table = cluster.keyspace(keyspace).table('table_name')
              expect(table.clustering_columns.map(&:name)).to eq(%w(region_number location))
            end
          end

          context 'with a different column structure' do
            let(:query) { 'CREATE TABLE table_name (section text, region_number int, location text, address text, PRIMARY KEY(section, region_number, location))' }
            let(:partition_key) { {'section' => 'text'} }
            let(:clustering_columns) { {'region_number' => 'int', 'location' => 'text'} }
            let(:fields) { {'address' => 'text'} }

            it 'should create the table with the specified columns' do
              subject.execute_async(query).get
              table = cluster.keyspace(keyspace).table('table_name')
              expect(table).to eq(expected_table)
            end
          end
        end

        describe 'with an INSERT query' do
          let(:keyspace) { 'development' }
          let(:table_keyspace) { keyspace }
          let(:table_name) { 'books' }
          let!(:table) do
            cluster.add_keyspace(keyspace)
            cluster.add_keyspace(table_keyspace)
            cluster.keyspace(table_keyspace).tap { |ks| ks.add_table(table_name, primary_key, columns) }.table(table_name)
          end
          let(:primary_key) { [['section'], 'genre'] }
          let(:columns) { {'section' => 'text', 'genre' => 'text', 'description' => 'text'} }
          let(:column_count) { columns.count }
          let(:column_names) { columns.keys }
          let(:column_values) { columns.keys.map { SecureRandom.uuid } }
          let(:expected_row) do
            column_count.times.inject({}) do |memo, index|
              memo.merge!(column_names[index] => column_values[index])
            end
          end
          let(:query) { "INSERT INTO #{table_name} (#{column_names*','}) VALUES (#{column_values*','})" }

          it 'should add a row into the specified table' do
            subject.execute_async(query).get
            expect(table.rows).to eq([expected_row])
          end

          context 'with a different table' do
            let(:table_name) { 'products' }

            it 'should add a row into the specified table' do
              subject.execute_async(query).get
              expect(table.rows).to eq([expected_row])
            end
          end

          context 'with a namespaced table' do
            let(:table_keyspace) { 'counters' }
            let(:query) { "INSERT INTO #{table_keyspace}.#{table_name} (#{column_names*','}) VALUES (#{column_values*','})" }

            it 'should add a row into the specified table' do
              subject.execute_async(query).get
              expect(table.rows).to eq([expected_row])
            end
          end
        end

        context 'with a TRUNCATE query' do
          let(:query) { "TRUNCATE #{table_name}" }
          let(:keyspace) { 'development' }
          let(:table_keyspace) { keyspace }
          let(:table_name) { 'books' }
          let!(:table) do
            cluster.add_keyspace(keyspace)
            cluster.add_keyspace(table_keyspace)
            cluster.keyspace(table_keyspace).tap do |ks|
              ks.add_table(table_name, [['pk1'], 'ck1'], {'pk1' => 'text', 'ck1' => 'text'})
            end.table(table_name)
          end

          before { table.insert('pk1' => 'hello', 'ck1' => 'world') }

          it 'should empty all rows in the table' do
            subject.execute_async(query).get
            expect(table.rows).to be_empty
          end

          context 'with a different table name' do
            let(:table_name) { 'products' }

            it 'should empty all rows in the table' do
              subject.execute_async(query).get
              expect(table.rows).to be_empty
            end
          end

          context 'with a namespaced table' do
            let(:table_keyspace) { 'counters' }
            let(:query) { "TRUNCATE #{table_keyspace}.#{table_name}" }

            it 'should empty all rows in the table' do
              subject.execute_async(query).get
              expect(table.rows).to be_empty
            end
          end
        end

        context 'with a SELECT query' do
          let(:query) { "SELECT * FROM #{table_name}" }
          let(:primary_key) { [['pk1'], 'ck1'] }
          let(:columns) { {'pk1' => 'text', 'ck1' => 'text', 'field1' => 'text'} }
          let(:keyspace) { 'development' }
          let(:table_keyspace) { keyspace }
          let(:table_name) { 'books' }
          let!(:table) do
            cluster.add_keyspace(keyspace)
            cluster.add_keyspace(table_keyspace)
            cluster.keyspace(table_keyspace).tap do |ks|
              ks.add_table(table_name, primary_key, columns)
            end.table(table_name)
          end
          let(:rows) do
            [{'pk1' => 'partition', 'ck1' => 'clustering', 'field1' => 'extra data'}]
          end

          before { rows.each { |row| table.insert(row) } }

          it 'should return the results of querying for rows from the table' do
            expect(subject.execute_async(query).get).to eq(table.rows)
          end

          context 'with different data in the table' do
            let(:rows) do
              [{'pk1' => 'other partition', 'ck1' => 'clustering', 'field1' => 'extra data'},
               {'pk1' => 'other partition', 'ck1' => 'other clustering', 'field1' => 'dreams field'}]
            end

            it 'should return the results of querying for rows from the table' do
              expect(subject.execute_async(query).get).to eq(table.rows)
            end
          end

          context 'with a different table' do
            let(:table_name) { 'food' }

            it 'should return the results of querying for rows from the table' do
              expect(subject.execute_async(query).get).to eq(table.rows)
            end
          end

          context 'with a namespaced table' do
            let(:table_keyspace) { 'counters' }
            let(:query) { "SELECT * FROM #{table_keyspace}.#{table_name}" }

            it 'should return the results of querying for rows from the table within the specified keyspace' do
              expect(subject.execute_async(query).get).to eq(table.rows)
            end
          end

          context 'when querying only specific rows' do
            let(:query) { 'SELECT pk1 FROM books' }

            it 'should return the results of querying for rows from the table containing only the specified columns' do
              expect(subject.execute_async(query).get).to eq([{'pk1' => 'partition'}])
            end

            context 'with different columns' do
              let(:query) { 'SELECT ck1, field1 FROM books' }

              it 'should return the results of querying for rows from the table containing only the specified columns' do
                expect(subject.execute_async(query).get).to eq([{'ck1' => 'clustering', 'field1' => 'extra data'}])
              end
            end
          end

          context 'with a filter' do
            let(:rows) do
              [{'pk1' => 'other partition', 'ck1' => 'clustering', 'field1' => 'extra data'},
               {'pk1' => 'other partition', 'ck1' => 'other clustering', 'field1' => 'dreams field'}]
            end
            let(:query) { "SELECT * FROM books WHERE pk1 = 'other partition' AND ck1 = 'clustering'" }

            it 'should filter the query results by the provided restriction' do
              expected_row = {'pk1' => 'other partition', 'ck1' => 'clustering', 'field1' => 'extra data'}
              expect(subject.execute_async(query).get).to eq([expected_row])
            end

            context 'with a different filter' do
              let(:query) { "SELECT * FROM books WHERE pk1 = 'other partition' AND ck1 = 'other clustering'" }

              it 'should filter the query results by the provided restriction' do
                expected_row = {'pk1' => 'other partition', 'ck1' => 'other clustering', 'field1' => 'dreams field'}
                expect(subject.execute_async(query).get).to eq([expected_row])
              end
            end

            context 'with a parameterized query' do
              let(:query) { 'SELECT * FROM books WHERE pk1 = ? AND ck1 = ?' }
              let(:statement) { subject.prepare(query) }

              it 'should run the pre-parsed query with the filled in parameters' do
                results = subject.execute_async(statement, 'other partition', 'clustering').get
                expected_row = {'pk1' => 'other partition', 'ck1' => 'clustering', 'field1' => 'extra data'}
                expect(results).to eq([expected_row])
              end
            end
          end
        end

        context 'when the query is a statement' do
          let(:query) { "CREATE KEYSPACE keyspace_name WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }" }
          let(:statement) { subject.prepare(query) }

          it 'should run the pre-parsed query' do
            subject.execute_async(statement).get
            expect(cluster.keyspace('keyspace_name')).to eq(Keyspace.new('keyspace_name'))
          end
        end
      end

      describe '#execute' do
        let(:future) { Cassandra::Future.value(:result) }
        let(:query) { 'SELECT * FROM everything' }
        before { allow(subject).to receive(:execute_async).with(query).and_return(future) }

        it 'should resolve the future of #execute_async' do
          expect(subject.execute(query)).to eq(:result)
        end

        context 'with a different query' do
          let(:future) { Cassandra::Future.value(['results']) }
          let(:query) { 'SELECT everything FROM nothing' }

          it 'should resolve the future of #execute_async' do
            expect(subject.execute(query)).to eq(['results'])
          end
        end
      end

      private

      def create_columns(columns)
        columns.map { |key, value| Cassandra::Column.new(key, value, :asc) }
      end

    end
  end
end
