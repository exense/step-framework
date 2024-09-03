Let's assume we have a pipeline with resolution = 500

Window examples:
range [0, 2000) =>this will create one bucket / source resolution. result keys = [0, 500, 1000, 1500)
range [0, 2000) + window(600) => window will be rounded to the closest source multiplier = 500. same result as above
range [0, 3000) + window(1200) => result keys = [0, 1000, 2000], and result resolution = 1000
range [400, 2000) => the range will alyways start from the lower source interval multiplier, so [0, 2000) in our case

Split examples:
range [0, 4000) + split(2) => result = [0, 2000)
range [0, 4000) + split(3) => range is transformed to [0, 4500], with resolution 1500, the result keys will be [0, 1500, 3000)
range [0, 4000) + split(7) => range is transformed to [0, 7000)  the resolution is 571, rounded to nearest multiple will be 500
range [0, 1000) + range(5) => this is a situation when the buckets count is also not respected, because the resolution don't allow us. [0, 500) is returned
