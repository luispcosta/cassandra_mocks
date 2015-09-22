require 'rspec'

module Cassandra
  module Mocks
    describe Session do
      let(:keyspace) { nil }

      subject { Session.new(keyspace) }

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

    end
  end
end
