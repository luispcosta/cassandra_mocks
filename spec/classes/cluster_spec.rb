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

    end
  end
end
