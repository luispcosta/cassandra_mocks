require 'spec_helper'

module Cassandra
  describe Uuid do

    describe '#<=>' do
      let(:lhs_id) { SecureRandom.uuid }
      let(:lhs) { Uuid.new(lhs_id) }
      let(:rhs_id) { SecureRandom.uuid }
      let(:rhs) { Uuid.new(rhs_id) }

      it 'should compare the uuids by their string value' do
        expect(lhs <=> rhs).to eq(lhs_id <=> rhs_id)
      end
    end

  end
end
