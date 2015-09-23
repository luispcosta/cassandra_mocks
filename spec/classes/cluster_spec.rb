require 'rspec'

module Cassandra
  module Mocks
    describe Cluster do

      describe '#add_keyspace' do
        it 'should add a keyspace with the specified name' do
          subject.add_keyspace('keyspace')
          expect(subject.keyspace('keyspace')).to eq(Keyspace.new('keyspace'))
        end

        context 'with a different keyspace name' do
          it 'should add a keyspace with the specified name' do
            subject.add_keyspace('other_keyspace')
            expect(subject.keyspace('other_keyspace')).to eq(Keyspace.new('other_keyspace'))
          end
        end
      end

      describe '#drop_keyspace' do
        let(:keyspace) { 'keyspace' }

        before { subject.add_keyspace(keyspace) }

        it 'should remove a keyspace with the specified name' do
          subject.drop_keyspace('keyspace')
          expect(subject.keyspace('keyspace')).to be_nil
        end

        context 'with a different keyspace name' do
          let(:keyspace) { 'counters' }

          it 'should add a keyspace with the specified name' do
            subject.drop_keyspace('counters')
            expect(subject.keyspace('counters')).to be_nil
          end
        end
      end

      describe '#each_keyspace' do
        let(:keyspaces) { %w(ks1 ks2) }

        before { keyspaces.each { |ks| subject.add_keyspace(ks) } }

        it 'should iterate over all keyspaces' do
          expect(subject.each_keyspace.map { |ks| ks }).to eq([Keyspace.new('ks1'), Keyspace.new('ks2')])
        end

        it 'should pass in the given block' do
          keyspaces = []
          subject.each_keyspace { |ks| keyspaces << ks }
          expect(keyspaces).to eq([Keyspace.new('ks1'), Keyspace.new('ks2')])
        end

        context 'with different keyspaces' do
          let(:keyspaces) { %w(other_ks1 other_ks2) }

          it 'should iterate over all keyspaces' do
            expect(subject.each_keyspace.map { |ks| ks }).to eq([Keyspace.new('other_ks1'), Keyspace.new('other_ks2')])
          end
        end
      end

    end
  end
end
