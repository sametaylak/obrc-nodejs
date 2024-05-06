# One billion row challenge

## Original Challenge
- https://github.com/gunnarmorling/1brc

## Usage
```console
$ npm run -s start > a.out
$ diff -w a.out measurements-100000.out
```

You should see nothing from the diff result.

## PC Specs
- CPU: AMD Ryzen 9 7950X
- Ram: 64GB
- SSD: Samsung SSD 990 PRO 2TB

## Results
- 1B row -> ~27.725s
- 10M row -> ~0.580s
- 1M row -> ~0.359s
- 100k row -> ~0.288s
