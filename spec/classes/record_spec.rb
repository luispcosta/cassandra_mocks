require 'rspec'

module Cassandra
  module Mocks
    describe Record do

      let(:values) { Faker::Lorem.words }

      subject { Record.new(values) }

      describe '#values' do
        its(:values) { is_expected.to eq(values) }
      end

      describe '#values=' do
        let(:new_values) { Faker::Lorem.words }

        before { subject.values = new_values }

        its(:values) { is_expected.to eq(new_values) }
      end

    end
  end
end