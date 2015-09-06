require 'rspec'

module Cassandra
  module Mocks
    describe Keyspace do

      let(:name) { 'keyspace' }
      let(:replication) { Cassandra::Keyspace::Replication.new('mock', {}) }
      let(:super_keyspace) { Cassandra::Keyspace.new(name, false, replication, []) }

      subject { Keyspace.new(name) }

      it { is_expected.to eq(super_keyspace) }

      context 'with a different keyspace' do
        let(:name) { 'other fancy keyspace' }

        it { is_expected.to eq(super_keyspace) }
      end
    end
  end
end