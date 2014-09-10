# Derrick

Inspect Redis databases and print statistics about the keys

## Installation

```shell
    $ gem install derrick
```

## Usage

```shell
$ derrick inspect redis://127.0.0.1:6379/5
```

It will print something like this:

```
Pattern                      Count  Exp Type
shop:*:name                  10000 100% string
shop:*:id                    10000   0% string
shop:*:versions                  2   0% string: 50%,hash: 50%
sorted_set:*:product_types       1   0% zset
```

You can also configure the concurrency level and batch size:

```shell
$ derrick inspect --concurrency 4 --batch-size 100000 redis://127.0.0.1:6379/5
```

## Contributing

1. Fork it ( https://github.com/[my-github-username]/derrick/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
