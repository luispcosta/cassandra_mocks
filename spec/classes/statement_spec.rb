require 'rspec'

module Cassandra
  module Mocks
    describe Statement do

      describe 'cql parsing' do
        it 'should save the input query CQL' do
          expect(Statement.new('SELECT * FROM everything', []).cql).to eq('SELECT * FROM everything')
        end

        context 'when the query is a CREATE query' do
          describe 'creating a table' do
            it 'should be parsed as a create_table' do
              statement = Statement.new('CREATE TABLE table_name (pk1 text PRIMARY KEY)', [])
              expect(statement.action).to eq(:create_table)
            end

            it 'should parse out the table name' do
              statement = Statement.new('CREATE TABLE table_name (pk1 text PRIMARY KEY)', [])
              expect(statement.args).to include(table: 'table_name')
            end

            context 'with a different table name' do
              it 'should parse out the table name' do
                statement = Statement.new('CREATE TABLE products (pk1 text PRIMARY KEY)', [])
                expect(statement.args).to include(table: 'products')
              end
            end

            it 'should parse out the column definitions' do
              statement = Statement.new('CREATE TABLE table_name (pk1 text PRIMARY KEY)', [])
              expect(statement.args).to include(columns: {'pk1' => 'text'})
            end

            it 'should set the primary key' do
              statement = Statement.new('CREATE TABLE table_name (pk1 text PRIMARY KEY)', [])
              expect(statement.args).to include(primary_key: [['pk1']])
            end

            context 'with a different set of columns' do
              it 'should parse all column definitions' do
                statement = Statement.new('CREATE TABLE products (type text PRIMARY KEY, section text)', [])
                expect(statement.args).to include(columns: {'type' => 'text', 'section' => 'text'})
              end
            end

            context 'when the primary key is defined at the end' do
              it 'should use that as the primary key' do
                statement = Statement.new('CREATE TABLE products (type text, section text, PRIMARY KEY(section))', [])
                expect(statement.args).to include(primary_key: [['section']])
              end
            end

            context 'with a complex primary key' do
              it 'should parse the clustering key as well' do
                statement = Statement.new('CREATE TABLE products (type text, section text, author text, PRIMARY KEY(type, section, author))', [])
                expect(statement.args).to include(primary_key: [['type'], 'section', 'author'])
              end

              context 'when the partition key is multi-part' do
                it 'should parse out the partition key properly' do
                  statement = Statement.new('CREATE TABLE products (type text, section text, PRIMARY KEY((type, section)))', [])
                  expect(statement.args).to include(primary_key: [['type', 'section']])
                end
              end
            end
          end

          describe 'creating a keyspace' do
            it 'should be parsed as a create_table' do
              statement = Statement.new("CREATE KEYSPACE keyspace_name WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }", [])
              expect(statement.action).to eq(:create_keyspace)
            end

            it 'should parse out the keyspace name' do
              statement = Statement.new("CREATE KEYSPACE keyspace_name WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }", [])
              expect(statement.args).to include(keyspace: 'keyspace_name')
            end

            context 'with a different keyspace' do
              it 'should parse out the keyspace name' do
                statement = Statement.new("CREATE KEYSPACE production_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }", [])
                expect(statement.args).to include(keyspace: 'production_keyspace')
              end
            end
          end
        end

        context 'when the query is an INSERT query' do
          it 'should be parsed as an insert' do
            statement = Statement.new("INSERT INTO table (  pk1, ck1  ) VALUES ('hello', ?)", [55])
            expect(statement.action).to eq(:insert)
          end

          it 'should map the key value pairs into the table from the parsed query' do
            statement = Statement.new("INSERT INTO table ( pk1, ck1 ) values ('hello', 'world')", [])
            expect(statement.args).to eq(keyspace: nil, table: 'table', values: {'pk1' => 'hello', 'ck1' => 'world'})
          end

          context 'with a namespaced table' do
            it 'should use the table within the specified keyspace' do
              statement = Statement.new("INSERT INTO keyspace.table ( pk1, ck1 ) values ('hello', 'world')", [])
              expect(statement.args).to eq(keyspace: 'keyspace', table: 'table', values: {'pk1' => 'hello', 'ck1' => 'world'})
            end
          end

          context 'with different arguments' do
            it 'should map the key value pairs into the table from the parsed query' do
              statement = Statement.new("  insert INTO other_table (category, message) VALUES   ('goodbye', 'cruel world')", [])
              expect(statement.args).to eq(keyspace: nil, table: 'other_table', values: {'category' => 'goodbye', 'message' => 'cruel world'})
            end
          end

          context 'with a parameterized query' do
            it 'should apply query parameters to the parsed query' do
              statement = Statement.new("INSERT INTO table (category, message, sub_message) VALUES ('goodbye', ?, ?)", %w(world cruel))
              expect(statement.args).to eq(keyspace: nil, table: 'table', values: {'category' => 'goodbye', 'message' => 'world', 'sub_message' => 'cruel'})
            end
          end
        end

        shared_examples_for 'a query with a restriction' do |keyword|

          context 'with a restriction' do
            it 'should parse the restriction as a column filter' do
              statement = Statement.new("#{keyword} FROM everything WHERE pk1 = 'books'", [])
              expect(statement.args).to include(filter: {'pk1' => 'books'})
            end

            it 'should support multiple restrictions' do
              statement = Statement.new("#{keyword} FROM everything WHERE pk1 = 'cds' and ck1 = 'Rock'", [])
              expect(statement.args).to include(filter: {'pk1' => 'cds', 'ck1' => 'Rock'})
            end

            context 'when the restriction provided is a range' do
              it 'should support range restrictions using IN' do
                statement = Statement.new("#{keyword} FROM everything WHERE pk1 = IN ('Videos', 'Games')", [])
                expect(statement.args).to include(filter: {'pk1' => %w(Videos Games)})
              end

              it 'should support parameterized restrictions' do
                statement = Statement.new("#{keyword} FROM everything WHERE pk1 = IN (?, 'Games') and ck1 = ?", %w(Videos History))
                expect(statement.args).to include(filter: {'pk1' => %w(Videos Games), 'ck1' => 'History'})
              end
            end

            it 'should support parameterized queries' do
              statement = Statement.new("#{keyword} FROM everything WHERE pk1 = 'cds' and ck1 = ?", ['Jazz'])
              expect(statement.args).to include(filter: {'pk1' => 'cds', 'ck1' => 'Jazz'})
            end
          end

        end

        shared_examples_for 'a query filtering a table' do |keyword|
          it 'should parse the table argument' do
            statement = Statement.new("#{keyword} FROM everything", [])
            expect(statement.args).to include(table: 'everything')
          end

          context 'with a different table' do
            it 'should parse the table and column arguments' do
              statement = Statement.new("#{keyword} FROM my_table", [])
              expect(statement.args).to include(table: 'my_table')
            end
          end

          context 'with a namespaced table' do
            it 'should parse the table and keyspace' do
              statement = Statement.new("#{keyword} FROM lockspace.my_table", [])
              expect(statement.args).to include(keyspace: 'lockspace', table: 'my_table')
            end
          end
        end

        context 'when the query is a SELECT query' do
          it 'should be parsed as a delete' do
            statement = Statement.new('SELECT * FROM everything', [55])
            expect(statement.action).to eq(:select)
          end

          it 'should parse the column argument' do
            statement = Statement.new('SELECT * FROM everything', [])
            expect(statement.args).to include(columns: %w(*))
          end

          context 'with different columns' do
            it 'should parse the table and column arguments' do
              statement = Statement.new('SELECT pk1, ck1, field1 FROM everything', [])
              expect(statement.args).to include(columns: %w(pk1 ck1 field1))
            end
          end

          it_behaves_like 'a query filtering a table', 'SELECT *'
          it_behaves_like 'a query with a restriction', 'SELECT *'
        end

        context 'when the query is a DELETE query' do
          it 'should be parsed as a delete' do
            statement = Statement.new('DELETE * FROM everything WHERE something = ?', %w(nothing))
            expect(statement.action).to eq(:delete)
          end

          it_behaves_like 'a query filtering a table', 'DELETE'
          it_behaves_like 'a query with a restriction', 'DELETE'
        end

      end

    end
  end
end
