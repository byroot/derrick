require 'spec_helper'

describe Derrick::Pattern do

  describe '.extract' do

    it 'canonicalizes integers' do
      expect('foo:42:bar').to match_pattern('foo:*:bar')
    end

    it 'canonicalizes MD5s' do
      expect('foo:258622b1688250cb619f3c9ccaefb7eb:bar').to match_pattern('foo:*:bar')
    end

    it 'canonicalizes SHA1s' do
      expect('foo:792eaaec6718c335d63cde10d7281baa4132ba78:bar').to match_pattern('foo:*:bar')
    end

    it 'escapes strings with invalid characters' do
      expect("hi \xAD").to match_pattern('hi \xAD')
    end

    it 'accepts `_` as segment separator' do
      expect('foo_42_bar').to match_pattern('foo_*_bar')
    end

    it 'accepts `/` as segment separator' do
      expect('foo/42/bar').to match_pattern('foo/*/bar')
    end

    context 'when no identifier is found' do

      it 'assumes the first segment is shared' do
        expect('foo:bar').to match_pattern('foo:*')
      end

      context 'and no segment serparator either' do

        it 'falls in the `*` pattern' do
          expect('foobar').to match_pattern('*')
        end

      end

    end

  end

end
