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

    end
  end
end
