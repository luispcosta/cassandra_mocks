require 'rspec'

module Cassandra
  module Mocks
    describe Keyspace do

      let(:name) { 'keyspace' }
      let(:replication) { Cassandra::Keyspace::Replication.new('mock', {}) }
      let(:super_keyspace) { Cassandra::Keyspace.new(name, false, replication, {}) }

      before do
        # hack to to compare empty tables
        allow(Concurrent::Map).to receive(:new).and_return({})
      end

      subject { Keyspace.new(name) }

      it { is_expected.to eq(super_keyspace) }

      context 'with a different keyspace' do
        let(:name) { 'other fancy keyspace' }

        it { is_expected.to eq(super_keyspace) }
      end

      describe '#add_table' do
        let(:columns) { {'pk1' => 'text', 'ck1' => 'int', 'field1' => 'double'} }
        let(:partition_key) { {'pk1' => 'text'} }
        let(:clustering_columns) { {'ck1' => 'int'} }
        let(:fields) { {'field1' => 'double'} }
        let(:primary_key) { [partition_key.keys, *clustering_columns.keys] }
        let(:table_name) { 'generic_table' }
        let(:ignore_existing) { false }
        let(:table) do
          Cassandra::Mocks::Table.new(name, table_name, create_columns(partition_key), create_columns(clustering_columns), create_columns(fields))
        end

        it 'should create a table using the specified parameters' do
          subject.add_table(table_name, primary_key, columns, ignore_existing)
          expect(subject.table(table_name)).to eq(table)
        end

        context 'when the table name is longer than 48 characters' do
          let(:table_name) { 'supercalafragalisticexpialadoshiosomobobbydobbydoo-1234567890-table' }

          it 'should raise an error indicating the table name is too long' do
            expect { subject.add_table(table_name, primary_key, columns, ignore_existing) }.to raise_error(Errors::InvalidError, "Table name '#{table_name}' cannot be greater than 48 characters")
          end
        end

        context 'when the table already exists' do
          it 'should raise an error' do
            subject.add_table(table_name, primary_key, columns, ignore_existing)
            expect { subject.add_table(table_name, primary_key, columns, ignore_existing) }.to raise_error(Errors::AlreadyExistsError, 'Cannot create already existing table')
          end

          context 'when we do not care' do
            let(:ignore_existing) { true }

            it 'should not raise an error' do
              subject.add_table(table_name, primary_key, columns, ignore_existing)
              expect { subject.add_table(table_name, primary_key, columns, ignore_existing) }.not_to raise_error
            end
          end
        end

        context 'with a different keyspace name' do
          let(:name) { 'production_keys' }

          it 'should create a table using the specified parameters' do
            subject.add_table(table_name, primary_key, columns, ignore_existing)
            expect(subject.table(table_name)).to eq(table)
          end
        end

        context 'with a different table name' do
          let(:table_name) { 'books' }

          it 'should create a table using the specified parameters' do
            subject.add_table(table_name, primary_key, columns, ignore_existing)
            expect(subject.table(table_name)).to eq(table)
          end
        end

        context 'with a different partition key' do
          let(:columns) { {'genre' => 'text', 'style' => 'text', 'ck1' => 'int', 'field1' => 'double'} }
          let(:partition_key) { {'genre' => 'text', 'style' => 'text'} }

          it 'should create a table using the specified parameters' do
            subject.add_table(table_name, primary_key, columns, ignore_existing)
            expect(subject.table(table_name).to_cql).to eq(table.to_cql)
          end
        end

        context 'with different clustering columns' do
          let(:columns) { {'pk1' => 'text', 'location_number' => 'int', 'district' => 'text', 'field1' => 'double'} }
          let(:clustering_columns) { {'district' => 'text', 'location_number' => 'int'} }

          it 'should create a table using the specified parameters' do
            subject.add_table(table_name, primary_key, columns, ignore_existing)
            expect(subject.table(table_name).to_cql).to eq(table.to_cql)
          end
        end

        context 'with different fields' do
          let(:columns) { {'pk1' => 'text', 'ck1' => 'int', 'green field' => 'double', 'blue field' => 'int'} }
          let(:fields) { {'green field' => 'double', 'blue field' => 'int'} }

          it 'should create a table using the specified parameters' do
            subject.add_table(table_name, primary_key, columns, ignore_existing)
            expect(subject.table(table_name).to_cql).to eq(table.to_cql)
          end
        end
      end

      describe '#drop_table' do
        let(:table_name) { 'table' }

        before { subject.add_table(table_name, [['pk1'], 'ck1'], {'pk1' => 'text', 'ck1' => 'text'}, false) }

        it 'should remove the table' do
          subject.drop_table('table')
          expect(subject.table('table')).to be_nil
        end

        context 'with a different table' do
          let(:table_name) { 'products' }

          it 'should remove the table' do
            subject.drop_table('products')
            expect(subject.table('products')).to be_nil
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
