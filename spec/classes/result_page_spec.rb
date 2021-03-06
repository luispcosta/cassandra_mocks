require 'rspec'

module Cassandra
  module Mocks
    describe ResultPage do

      it { is_expected.to be_a_kind_of(Array) }

      describe '#last_page?' do
        its(:last_page?) { is_expected.to eq(true) }
      end

      describe '#next_page' do
        its(:next_page) { is_expected.to be_nil }
      end

      describe '#next_page_async' do
        subject { ResultPage.new.next_page_async }

        it { is_expected.to be_a_kind_of(Cassandra::Future) }
        its(:get) { is_expected.to be_nil }
      end

      describe '#paging_state' do
        its(:paging_state) { is_expected.to be_nil }
      end

      describe '#execution_info' do
        its(:execution_info) { is_expected.to eq({}) }

        context 'when overridden' do
          let(:execution_info) { Faker::Lorem.words.inject({}) { |memo, key| memo.merge!(key => Faker::Lorem.sentence) } }
          before { subject.execution_info = execution_info }

          its(:execution_info) { is_expected.to eq(execution_info) }
        end
      end

    end
  end
end
