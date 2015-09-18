require 'rspec'

module Cassandra
  module Mocks
    class Statement
      describe Token do
        let(:type) { :insert }
        let(:value) { 'INSERT' }

        subject { Token.new(type, value) }

        describe 'attributes' do
          its(:type) { is_expected.to eq(:insert) }
          its(:value) { is_expected.to eq('INSERT') }

          context 'with a different type set' do
            let(:type) { :select }
            let(:value) { 'SELECT' }

            its(:type) { is_expected.to eq(:select) }
            its(:value) { is_expected.to eq('SELECT') }
          end
        end

      end
    end
  end
end
