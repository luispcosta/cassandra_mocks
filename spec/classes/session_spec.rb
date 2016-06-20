require 'rspec'

module Cassandra
  module Mocks
    describe Session do
      let(:keyspace) { nil }
      let(:cluster) { Cluster.new }
      let(:session) { Session.new(keyspace, cluster) }

      subject { session }

      its(:cluster) { is_expected.to eq(cluster) }

      describe '#close_async' do
        subject { session.close_async }

        it { is_expected.to be_a_kind_of(Cassandra::Future) }

        its(:get) { is_expected.to be_nil }
      end

      describe '#close' do
        subject { session.close }

        it { is_expected.to be_nil }
      end

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
        let(:query) { "CREATE KEYSPACE fake WITH REPLICATION = { 'class' : 'EXTREME', 'replication_factor' : 33333 }" }

        it 'should return a Cassandra::Future' do
          expect(subject.execute_async(query)).to be_a_kind_of(Cassandra::Future)
        end

        it 'should resolve to an empty array' do
          expect(subject.execute_async(query).get).to eq([])
        end

        it 'should save the query options in the result #execution_info' do
          options = Faker::Lorem.words.inject({}) { |memo, key| memo.merge!(key => Faker::Lorem.sentence) }
          expect(subject.execute_async(query, options).get.execution_info).to eq(options)
        end

        describe 'with a CREATE KEYSPACE query' do
          let(:query) { "CREATE KEYSPACE keyspace_name WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }" }

          before do
            # hack to to compare empty tables
            allow(Concurrent::Map).to receive(:new).and_return({})
          end

          it 'should create the keyspace' do
            subject.execute_async(query).get
            expect(cluster.keyspace('keyspace_name')).to eq(Keyspace.new('keyspace_name'))
          end

          describe 'a IF NOT EXISTS query' do
            let(:query) { "CREATE KEYSPACE IF NOT EXISTS keyspace_name WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }" }
            before { subject.execute_async(query).get }

            it 'should not raise an error' do
              expect { subject.execute_async(query).get }.not_to raise_error
            end
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

          before { cluster.add_keyspace(keyspace, false) }

          it 'should create the table with the specified params' do
            subject.execute_async(query).get
            table = cluster.keyspace(keyspace).table('table_name')
            expect(table).to eq(expected_table)
          end

          describe 'a IF NOT EXISTS query' do
            let(:query) { "CREATE TABLE IF NOT EXISTS #{table_name} (pk1 text, ck1 text, field1 text, PRIMARY KEY(pk1, ck1))" }
            before { subject.execute_async(query).get }

            it 'should not raise an error' do
              expect { subject.execute_async(query).get }.not_to raise_error
            end
          end

          describe 'creating counter tables' do
            let(:query) { "CREATE TABLE #{table_name} (pk1 text, ck1 text, field1 counter, PRIMARY KEY(pk1, ck1))" }

            it 'should not include the primary key as part of the fields' do
              expect { subject.execute_async(query).get }.not_to raise_error
            end
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
            cluster.add_keyspace(keyspace, false)
            cluster.add_keyspace(table_keyspace, true)
            cluster.keyspace(table_keyspace).tap { |ks| ks.add_table(table_name, primary_key, columns, false) }.table(table_name)
          end
          let(:primary_key) { [['section'], 'genre'] }
          let(:field_type) { 'text' }
          let(:columns) { {'section' => 'text', 'genre' => 'text', 'description' => field_type} }
          let(:column_count) { columns.count }
          let(:column_names) { columns.keys }
          let(:column_values) { columns.keys.map { SecureRandom.uuid } }
          let(:quoted_column_values) { column_values.map { |value| "'#{value}'" } }
          let(:expected_row) do
            column_count.times.inject({}) do |memo, index|
              memo.merge!(column_names[index] => column_values[index])
            end
          end
          let(:query) { "INSERT INTO #{table_name} (#{column_names*','}) VALUES (#{quoted_column_values*','})" }

          it 'should add a row into the specified table' do
            subject.execute_async(query).get
            expect(table.rows).to eq([expected_row])
          end

          it 'should return a ResultPage' do
            expect(subject.execute_async(query).get).to be_a_kind_of(ResultPage)
          end

          it 'should resolve to an empty ResultPage' do
            expect(subject.execute_async(query).get).to eq([])
          end

          context 'with a counter table' do
            let(:field_type) { 'counter' }

            it 'should raise and InvalidError' do
              expect { subject.execute_async(query).get }.to raise_error(Cassandra::Errors::InvalidError, 'INSERT statement are not allowed on counter tables, use UPDATE instead')
            end
          end

          context 'when checking if the record already exists' do
            let(:query) { "INSERT INTO #{table_name} (#{column_names*','}) VALUES (#{quoted_column_values*','}) IF NOT EXISTS" }

            it 'should resolve to a hash indicating that the record was inserted successfully' do
              expect(subject.execute_async(query).get).to eq([{'[applied]' => true}])
            end

            context 'when the row has already been inserted' do
              let(:old_row) { expected_row.merge('description' => 'introduction to duplicate records') }

              before { table.insert(old_row) }

              it 'should add a row into the specified table' do
                subject.execute_async(query).get
                expect(table.rows).to eq([old_row])
              end

              it 'should resolve to a hash indicating a failure to insert the record' do
                expect(subject.execute_async(query).get).to eq([{'[applied]' => false}])
              end
            end
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
            let(:query) { "INSERT INTO #{table_keyspace}.#{table_name} (#{column_names*','}) VALUES (#{quoted_column_values*','})" }

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
            cluster.add_keyspace(keyspace, false)
            cluster.add_keyspace(table_keyspace, true)
            cluster.keyspace(table_keyspace).tap do |ks|
              ks.add_table(table_name, [['pk1'], 'ck1'], {'pk1' => 'text', 'ck1' => 'text'}, false)
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

        context 'with a DROP keyspace query' do
          let(:keyspace) { 'keys' }
          let(:query) { "DROP KEYSPACE #{keyspace}" }

          before { cluster.add_keyspace(keyspace, false) }

          it 'should delete the keyspace' do
            subject.execute_async(query).get
            expect(cluster.keyspace(keyspace)).to be_nil
          end

          context 'with a different keyspace' do
            let(:keyspace) { 'locks' }

            it 'should delete the keyspace' do
              subject.execute_async(query).get
              expect(cluster.keyspace(keyspace)).to be_nil
            end
          end
        end

        context 'with a DROP table query' do
          let(:keyspace) { 'keys' }
          let(:table_keyspace) { keyspace }
          let(:table_name) { 'table' }
          let!(:table) do
            cluster.add_keyspace(keyspace, false)
            cluster.add_keyspace(table_keyspace, true)
            cluster.keyspace(table_keyspace).tap do |ks|
              ks.add_table(table_name, [['pk1'], 'ck1'], {'pk1' => 'text', 'ck1' => 'text'}, false)
            end.table(table_name)
          end
          let(:query) { "DROP TABLE #{table_name}" }
          let(:keyspace_with_table) { cluster.keyspace(table_keyspace) }

          it 'should delete the table' do
            subject.execute_async(query).get
            expect(keyspace_with_table.table(table_name)).to be_nil
          end

          context 'with a different keyspace' do
            let(:table_name) { 'prices' }

            it 'should delete the table' do
              subject.execute_async(query).get
              expect(keyspace_with_table.table(table_name)).to be_nil
            end
          end

          context 'with a namespaced table' do
            let(:table_keyspace) { 'counters' }
            let(:query) { "DROP TABLE #{table_keyspace}.#{table_name}" }
            let(:table_name) { 'prices' }

            it 'should delete the table' do
              subject.execute_async(query).get
              expect(keyspace_with_table.table(table_name)).to be_nil
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
            cluster.add_keyspace(keyspace, false)
            cluster.add_keyspace(table_keyspace, true)
            cluster.keyspace(table_keyspace).tap do |ks|
              ks.add_table(table_name, primary_key, columns, false)
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

          context 'with a filter or limit' do
            let(:rows) do
              [{'pk1' => 'other partition', 'ck1' => 'clustering', 'field1' => 'extra data'},
               {'pk1' => 'other partition', 'ck1' => 'other clustering', 'field1' => 'dreams field'}]
            end
            let(:query) { "SELECT * FROM books WHERE pk1 = 'other partition' AND ck1 = 'clustering'" }

            it 'should filter the query results by the provided restriction' do
              expected_row = {'pk1' => 'other partition', 'ck1' => 'clustering', 'field1' => 'extra data'}
              expect(subject.execute_async(query).get).to eq([expected_row])
            end

            describe 'handling limits' do
              let(:query) { 'SELECT * FROM books limit 1' }

              it 'should filter the query results by the provided restriction' do
                expected_row = {'pk1' => 'other partition', 'ck1' => 'clustering', 'field1' => 'extra data'}
                expect(subject.execute_async(query).get).to eq([expected_row])
              end
            end

            describe 'handling column ordering' do
              let(:query) { 'SELECT * FROM books ORDER BY ck1 DESC LIMIT 1' }

              it 'should filter the query results by the provided restriction' do
                expected_row = {'pk1' => 'other partition', 'ck1' => 'other clustering', 'field1' => 'dreams field'}
                expect(subject.execute_async(query).get).to eq([expected_row])
              end
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
                results = subject.execute_async(statement, arguments: ['other partition', 'clustering']).get
                expected_row = {'pk1' => 'other partition', 'ck1' => 'clustering', 'field1' => 'extra data'}
                expect(results).to eq([expected_row])
              end

              it 'should support splat args' do
                results = subject.execute_async(statement, 'other partition', 'clustering').get
                expected_row = {'pk1' => 'other partition', 'ck1' => 'clustering', 'field1' => 'extra data'}
                expect(results).to eq([expected_row])
              end
            end
          end
        end

        context 'with an UPDATE query' do
          let(:primary_key) { [['pk1'], 'ck1'] }
          let(:columns) { {'pk1' => 'text', 'ck1' => 'text', 'field1' => 'int'} }
          let(:keyspace) { 'development' }
          let(:table_keyspace) { keyspace }
          let(:table_name) { 'books' }
          let!(:table) do
            cluster.add_keyspace(keyspace, false)
            cluster.add_keyspace(table_keyspace, true)
            cluster.keyspace(table_keyspace).tap do |ks|
              ks.add_table(table_name, primary_key, columns, false)
            end.table(table_name)
          end
          let(:row) { {'pk1' => 'books', 'ck1' => 'mystery', 'field1' => 5} }
          let(:query) { "UPDATE #{table_name} SET field1 = 7 WHERE pk1 = 'books' AND ck1 = 'mystery'" }

          before { table.insert(row) }

          it 'should update the row with the specified values' do
            subject.execute_async(query).get
            expected_row = row.merge('field1' => 7)
            expect(table.select('*')).to eq([expected_row])
          end

          context 'when the row does not already exist' do
            let(:query) { "UPDATE #{table_name} SET field1 = 7 WHERE pk1 = 'books' AND ck1 = 'romance'" }

            it 'should update the row with the specified values' do
              subject.execute_async(query).get
              expected_row = row.merge('ck1' => 'romance', 'field1' => 7)
              expect(table.select('*', restriction: {'pk1' => 'books', 'ck1' => 'romance'})).to eq([expected_row])
            end
          end

          context 'with a different table' do
            let(:table_name) { 'book_counts' }

            it 'should update the row with the specified values' do
              subject.execute_async(query).get
              expected_row = row.merge('field1' => 7)
              expect(table.select('*')).to eq([expected_row])
            end
          end

          context 'with a namespaced table' do
            let(:table_keyspace) { 'production_counters' }
            let(:query) { "UPDATE #{table_keyspace}.#{table_name} SET field1 = 7 WHERE pk1 = 'books' AND ck1 = 'mystery'" }

            it 'should update the row with the specified values' do
              subject.execute_async(query).get
              expected_row = row.merge('field1' => 7)
              expect(table.select('*')).to eq([expected_row])
            end
          end

          context 'with a different filter' do
            let(:query) { "UPDATE #{table_name} SET field1 = 7 WHERE pk1 = 'movies' AND ck1 = 'action'" }
            let(:row) { {'pk1' => 'movies', 'ck1' => 'action', 'field1' => 5} }

            it 'should update the row with the specified values' do
              subject.execute_async(query).get
              expected_row = row.merge('field1' => 7)
              expect(table.select('*')).to eq([expected_row])
            end
          end

          context 'with a different update value' do
            let(:query) { "UPDATE #{table_name} SET field1 = 17 WHERE pk1 = 'books' AND ck1 = 'mystery'" }

            it 'should update the row with the specified values' do
              subject.execute_async(query).get
              expected_row = row.merge('field1' => 17)
              expect(table.select('*')).to eq([expected_row])
            end
          end

          context 'with different fields' do
            let(:columns) { {'pk1' => 'text', 'ck1' => 'text', 'book_count' => 'int'} }
            let(:row) { {'pk1' => 'books', 'ck1' => 'mystery', 'book_count' => 51234} }
            let(:query) { "UPDATE #{table_name} SET book_count = 70101 WHERE pk1 = 'books' AND ck1 = 'mystery'" }

            it 'should update the row with the specified values' do
              subject.execute_async(query).get
              expected_row = row.merge('book_count' => 70101)
              expect(table.select('*')).to eq([expected_row])
            end
          end

          context 'with multiple rows to update' do
            let(:row_two) { {'pk1' => 'books', 'ck1' => 'romance', 'field1' => 17} }
            let(:query) { "UPDATE #{table_name} SET field1 = 21 WHERE pk1 = 'books'" }

            before { table.insert(row_two) }

            it 'should update both rows' do
              subject.execute_async(query).get
              expected_row = row.merge('field1' => 21)
              expected_row_two = row_two.merge('field1' => 21)
              expect(table.select('*')).to eq([expected_row, expected_row_two])
            end
          end

          context 'when the update contains arithmetic' do
            let(:field_type) { 'counter' }
            let(:columns) { {'pk1' => 'text', 'ck1' => 'text', 'field1' => field_type} }
            let(:row) { {'pk1' => 'books', 'ck1' => 'mystery', 'field1' => 17} }
            let(:query) { "UPDATE #{table_name} SET field1 = field1 + 3 WHERE pk1 = 'books'" }

            it 'should apply the arithmetic to the specified field' do
              subject.execute_async(query).get
              expected_row = row.merge('field1' => 20)
              expect(table.select('*')).to eq([expected_row])
            end

            context 'when the updated field is not a counter' do
              let(:field_type) { 'int' }

              it 'should raise and InvalidError' do
                expect { subject.execute_async(query).get }.to raise_error(Cassandra::Errors::InvalidError, 'Invalid operation (field1 = field1 + ?) for non counter column field1')
              end
            end
          end
        end

        context 'with a DELETE query' do
          let(:primary_key) { [['pk1'], 'ck1'] }
          let(:columns) { {'pk1' => 'text', 'ck1' => 'text', 'field1' => 'int'} }
          let(:keyspace) { 'development' }
          let(:table_keyspace) { keyspace }
          let(:table_name) { 'books' }
          let!(:table) do
            cluster.add_keyspace(keyspace, false)
            cluster.add_keyspace(table_keyspace, true)
            cluster.keyspace(table_keyspace).tap do |ks|
              ks.add_table(table_name, primary_key, columns, false)
            end.table(table_name)
          end
          let(:row) { {'pk1' => 'books', 'ck1' => 'mystery', 'field1' => 5} }
          let(:query) { "DELETE FROM #{table_name} WHERE pk1 = 'books' AND ck1 = 'mystery'" }

          before { table.insert(row) }

          it 'should delete the row with the specified values' do
            subject.execute_async(query).get
            expect(table.select('*')).to be_empty
          end

          context 'with a different table' do
            let(:table_name) { 'book_counts' }

            it 'should delete the row with the specified values' do
              subject.execute_async(query).get
              expect(table.select('*')).to be_empty
            end
          end

          context 'with a namespaced table' do
            let(:table_keyspace) { 'production_counters' }
            let(:query) { "DELETE FROM #{table_keyspace}.#{table_name} WHERE pk1 = 'books' AND ck1 = 'mystery'" }

            it 'should delete the row with the specified values' do
              subject.execute_async(query).get
              expect(table.select('*')).to be_empty
            end
          end

          context 'with a different filter' do
            let(:query) { "DELETE FROM #{table_name} WHERE pk1 = 'movies' AND ck1 = 'action'" }
            let(:row) { {'pk1' => 'movies', 'ck1' => 'action', 'field1' => 5} }

            it 'should delete the row with the specified values' do
              subject.execute_async(query).get
              expect(table.select('*')).to be_empty
            end
          end

          context 'with multiple rows on different primary keys' do
            let(:row_two) { {'pk1' => 'movies', 'ck1' => 'romance', 'field1' => 17} }
            let(:query) { "DELETE FROM #{table_name} WHERE pk1 = 'books'" }

            before { table.insert(row_two) }

            it 'should delete both rows' do
              subject.execute_async(query).get
              expect(table.select('*')).to eq([row_two])
            end
          end

          context 'with multiple rows to delete' do
            let(:row_two) { {'pk1' => 'books', 'ck1' => 'romance', 'field1' => 17} }
            let(:query) { "DELETE FROM #{table_name} WHERE pk1 = 'books'" }

            before { table.insert(row_two) }

            it 'should delete both rows' do
              subject.execute_async(query).get
              expect(table.select('*')).to be_empty
            end
          end
        end

        context 'when the query is a statement' do
          let(:query) { "CREATE KEYSPACE keyspace_name WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }" }
          let(:statement) { subject.prepare(query) }

          before do
            # hack to to compare empty tables
            allow(Concurrent::Map).to receive(:new).and_return({})
          end

          it 'should run the pre-parsed query' do
            subject.execute_async(statement).get
            expect(cluster.keyspace('keyspace_name')).to eq(Keyspace.new('keyspace_name'))
          end
        end

        context 'when the query is a batch' do
          let(:query) { 'INSERT INTO records (key, value) VALUES (?, ?)' }
          let(:query_args) { %w(abc def) }
          let(:dummy_future) { Cassandra::Future.value(true) }
          let(:statement) { Cassandra::Statements::Batch.new }

          before do
            statement.add(query, arguments: query_args)
            allow(subject).to receive(:execute_async).and_call_original
            allow(subject).to receive(:execute_async).with(query, arguments: query_args).and_return(dummy_future)
          end

          it 'should execute all the underlying queries' do
            expect(subject).to receive(:execute_async).with(query, arguments: query_args).and_return(dummy_future)
            subject.execute_async(statement)
          end

          it 'should return a ResultPage' do
            expect(subject.execute_async(statement).get).to be_a_kind_of(ResultPage)
          end

          context 'with multiple statements in the batch' do
            let(:query_two) { 'INSERT INTO other_records (key, value) VALUES (?, ?)' }
            let(:query_two_statement) { subject.prepare(query_two) }
            let(:query_two_args) { %w(ghi jkl) }

            before do
              statement.add(query_two_statement, arguments: query_two_args)
            end

            it 'should execute all the underlying queries' do
              expect(subject).to receive(:execute_async).with(query_two_statement, arguments: query_two_args).and_return(dummy_future)
              subject.execute_async(statement)
            end
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
        columns.map { |column, type| Cassandra::Column.new(column, type.to_sym, :asc) }
      end

    end
  end
end
