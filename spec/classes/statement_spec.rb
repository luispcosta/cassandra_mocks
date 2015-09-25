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

        context 'when the query is a TRUNCATE query' do
          it 'should be parsed as a truncate' do
            statement = Statement.new('TRUNCATE table', [])
            expect(statement.action).to eq(:truncate)
          end

          it 'should parse out the table name' do
            statement = Statement.new('TRUNCATE table', [])
            expect(statement.args).to include(table: 'table')
          end

          context 'with a different table name' do
            it 'should parse out the table name' do
              statement = Statement.new('TRUNCATE books', [])
              expect(statement.args).to include(table: 'books')
            end
          end

          context 'with a namespaced table' do
            it 'should parse out the table name and keyspace' do
              statement = Statement.new('TRUNCATE keyspace.books', [])
              expect(statement.args).to include(keyspace: 'keyspace', table: 'books')
            end

            context 'with a different keyspace' do
              it 'should parse out the table name and keyspace' do
                statement = Statement.new('TRUNCATE staging.products', [])
                expect(statement.args).to include(keyspace: 'staging', table: 'products')
              end
            end
          end
        end

        context 'when the query is a DROP query' do
          context 'when dropping a TABLE' do
            it 'should be parsed as a drop table' do
              statement = Statement.new('DROP TABLE table', [])
              expect(statement.action).to eq(:drop_table)
            end

            it 'should parse out the table name' do
              statement = Statement.new('DROP TABLE table', [])
              expect(statement.args).to include(table: 'table')
            end

            context 'with a different table name' do
              it 'should parse out the table name' do
                statement = Statement.new('DROP TABLE books', [])
                expect(statement.args).to include(table: 'books')
              end
            end

            context 'with a namespaced table' do
              it 'should parse out the table name and keyspace' do
                statement = Statement.new('DROP TABLE keyspace.books', [])
                expect(statement.args).to include(keyspace: 'keyspace', table: 'books')
              end

              context 'with a different keyspace' do
                it 'should parse out the table name and keyspace' do
                  statement = Statement.new('DROP TABLE staging.products', [])
                  expect(statement.args).to include(keyspace: 'staging', table: 'products')
                end
              end
            end
          end

          context 'when dropping a KEYSPACE' do
            it 'should be parsed as a drop keyspace' do
              statement = Statement.new('DROP KEYSPACE keys', [])
              expect(statement.action).to eq(:drop_keyspace)
            end

            it 'should parse out the keyspace name' do
              statement = Statement.new('DROP KEYSPACE keys', [])
              expect(statement.args).to include(keyspace: 'keys')
            end

            context 'with a different keyspace name' do
              it 'should parse out the keyspace name' do
                statement = Statement.new('DROP KEYSPACE counters', [])
                expect(statement.args).to include(keyspace: 'counters')
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

          context 'with numeric parameters' do
            it 'should map the key value pairs into the table from the parsed query' do
              statement = Statement.new('INSERT INTO other_table (category, message_index) VALUES   (5, 102443)', [])
              expect(statement.args).to eq(keyspace: nil, table: 'other_table', values: {'category' => 5, 'message_index' => 102443})
            end
          end

          context 'with a parameterized query' do
            it 'should apply query parameters to the parsed query' do
              statement = Statement.new("INSERT INTO table (category, message, sub_message) VALUES ('goodbye', ?, ?)", %w(world cruel))
              expect(statement.args).to eq(keyspace: nil, table: 'table', values: {'category' => 'goodbye', 'message' => 'world', 'sub_message' => 'cruel'})
            end
          end

          context 'when checking if the record already exists' do
            it 'should include an option to describe this behaviour' do
              statement = Statement.new("INSERT INTO table (pk1, ck1) VALUES (hello, world) IF NOT EXISTS", [])
              expect(statement.args).to include(check_exists: true)
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

            context 'when the filter contains numerics' do
              it 'should parse the filter' do
                statement = Statement.new("#{keyword} FROM everything WHERE pk1 = 5 and ck1 = 4.23", [])
                expect(statement.args).to include(filter: {'pk1' => 5, 'ck1' => 4.23})
              end
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
          it 'should be parsed as a select' do
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

          context 'with a LIMIT provided' do
            it 'should parse the restriction as a column filter with the specified limit' do
              statement = Statement.new("SELECT * FROM everything WHERE pk1 = 'books' LIMIT 5", [])
              expect(statement.args).to include(limit: 5)
            end

            it 'should parse specified limit' do
              statement = Statement.new('SELECT * FROM everything LIMIT 3', [])
              expect(statement.args).to include(limit: 3)
            end
          end
        end

        context 'when the query is a DELETE query' do
          it 'should be parsed as a delete' do
            statement = Statement.new('DELETE FROM everything WHERE something = ?', %w(nothing))
            expect(statement.action).to eq(:delete)
          end

          it_behaves_like 'a query filtering a table', 'DELETE'
          it_behaves_like 'a query with a restriction', 'DELETE'
        end

        context 'when the query is an UPDATE query' do
          it 'should be parsed as a update' do
            statement = Statement.new('UPDATE table SET field1 = 55 WHERE pk1 = 19', [])
            expect(statement.action).to eq(:update)
          end

          it 'should parse out the table name' do
            statement = Statement.new('UPDATE table SET field1 = 55 WHERE pk1 = 19', [])
            expect(statement.args).to include(table: 'table')
          end

          context 'with a different table' do
            it 'should parse out the table name' do
              statement = Statement.new('UPDATE products SET field1 = 55 WHERE pk1 = 19', [])
              expect(statement.args).to include(table: 'products')
            end
          end

          context 'with a namespaced table' do
            it 'should parse out the table name and keyspace' do
              statement = Statement.new('UPDATE keys.products SET field1 = 55 WHERE pk1 = 19', [])
              expect(statement.args).to include(keyspace: 'keys', table: 'products')
            end

            context 'with a different keyspace' do
              it 'should parse out the table name and keyspace' do
                statement = Statement.new('UPDATE counters.product_counts SET field1 = 55 WHERE pk1 = 19', [])
                expect(statement.args).to include(keyspace: 'counters', table: 'product_counts')
              end
            end

            it 'should parse out the columns to update with the specified values' do
              statement = Statement.new('UPDATE keys.products SET field1 = 55 WHERE pk1 = 19', [])
              expect(statement.args).to include(values: {'field1' => 55})
            end
          end

          it 'should parse out the columns to update with the specified values' do
            statement = Statement.new('UPDATE table SET field1 = 55 WHERE pk1 = 45', [])
            expect(statement.args).to include(values: {'field1' => 55})
          end

          context 'with a different column update' do
            it 'should parse out the columns to update with the specified values' do
              statement = Statement.new("UPDATE table SET other_field = 47, description = 'great!' WHERE pk1 = 'partitioner'", [])
              expect(statement.args).to include(values: {'other_field' => 47, 'description' => 'great!'})
            end
          end

          context 'with a counter update' do
            it 'should parse out the column operation' do
              statement = Statement.new("UPDATE table SET other_field = other_field+1 WHERE pk1 = 'partitioner'", [])
              expect(statement.args).to include(values: {'other_field' => Statement::Arithmetic.new(:plus, 'other_field', 1)})
            end

            context 'with a different operator' do
              it 'should parse out the column operation' do
                statement = Statement.new("UPDATE table SET other_field = other_field-5 WHERE pk1 = 'partitioner'", [])
                expect(statement.args).to include(values: {'other_field' => Statement::Arithmetic.new(:minus, 'other_field', 5)})
              end
            end

            context 'with a parameterized query' do
              it 'should parse out the column operation' do
                statement = Statement.new("UPDATE table SET other_field = other_field + ? WHERE pk1 = 'partitioner'", [7])
                expect(statement.args).to include(values: {'other_field' => Statement::Arithmetic.new(:plus, 'other_field', 7)})
              end
            end
          end

          describe 'update filtering' do
            it 'should parse the restriction as a column filter' do
              statement = Statement.new("UPDATE everything SET field1 = 55 WHERE pk1 = 'books'", [])
              expect(statement.args).to include(filter: {'pk1' => 'books'})
            end

            it 'should support multiple restrictions' do
              statement = Statement.new("UPDATE everything SET field1 = 55 WHERE pk1 = 'cds' and ck1 = 'Rock'", [])
              expect(statement.args).to include(filter: {'pk1' => 'cds', 'ck1' => 'Rock'})
            end
          end
        end

        describe 'queries to be filled in later' do
          context 'when arguments are specified at a later time' do
            subject { Statement.new('DELETE FROM everything WHERE something = ?', []) }

            it 'should nullify the cql params' do
              expect(subject.args).to include(filter: {'something' => nil})
            end
          end
        end
      end

      describe '#fill_params' do
        let(:args) { %w(nothing something) }
        let(:original_statement) { Statement.new('DELETE FROM everything WHERE something = ? AND nothing = ?', []) }

        subject { original_statement.fill_params(args) }

        it 'should duplicate the cql' do
          expect(subject.cql).to eq(original_statement.cql)
        end

        it 'should duplicate the action' do
          expect(subject.action).to eq(original_statement.action)
        end

        it 'should duplicate the args, with the params filled in' do
          expect(subject.args).to eq(original_statement.args.merge(filter: {'something' => 'nothing', 'nothing' => 'something'}))
        end

        context 'with different args' do
          let(:args) { ['something good', 'nothing bad'] }

          it 'should duplicate the args, with the params filled in' do
            expect(subject.args).to eq(original_statement.args.merge(filter: {'something' => 'something good', 'nothing' => 'nothing bad'}))
          end
        end

        context 'with a different query' do
          let(:original_statement) do
            Statement.new("SELECT * FROM everything WHERE section = ? AND genre = 'Romance' AND shard = ?", [])
          end
          let(:args) { %w(Books abcdefg) }

          it 'should duplicate the args, with the params filled in' do
            expected_filter = {'section' => 'Books', 'genre' => 'Romance', 'shard' => 'abcdefg'}
            expect(subject.args).to eq(original_statement.args.merge(filter: expected_filter))
          end
        end

        context 'with an INSERT query' do
          let(:original_statement) do
            Statement.new('INSERT INTO table (pk1, ck1) VALUES (?, ?)', [])
          end
          let(:args) { %w(Books abcdefg) }

          it 'should duplicate the args, with the params filled in' do
            expected_values = {'pk1' => 'Books', 'ck1' => 'abcdefg'}
            expect(subject.args).to eq(original_statement.args.merge(values: expected_values))
          end
        end

        context 'with a query that does not require params' do
          let(:original_statement) do
            Statement.new('CREATE TABLE table(pk1 text, ck1 text, (pk1, ck1))', [])
          end
          let(:args) { [] }

          it 'should leave the args alone' do
            expect(subject.args).to eq(original_statement.args)
          end
        end
      end

    end
  end
end
