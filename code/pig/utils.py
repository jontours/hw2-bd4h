@outputSchema("feature:chararray")
def bag_to_svmlight(input):
    return ' '.join(( "%s:%f" % (fid, float(fvalue)) for _, fid, fvalue in input))

@outputSchema("indexes:bag{t:(index:int,eventid:chararray)}")
def bag_to_indexed(input):
	values = []
	index = 0
	for value in reversed(input):
		values.append((index,value[0]))
		index += 1
	return values


