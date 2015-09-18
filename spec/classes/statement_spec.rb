require 'rspec'

module Cassandra
  module Mocks
    describe Statement do

      describe 'cql parsing' do
        it 'should save the input query CQL' do
          expect(Statement.new('SELECT * FROM everything', []).cql).to eq('SELECT * FROM everything')
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

      end

    end
  end
end
