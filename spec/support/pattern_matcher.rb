RSpec::Matchers.define :match_pattern do |expected_pattern|
  match do |actual|
    Derrick::Pattern.extract(actual) == expected_pattern
  end

  failure_message do |actual|
    "expected `#{actual}` to be canonicalized as `#{expected_pattern}` but was `#{Derrick::Pattern.extract(actual)}`"
  end
end
