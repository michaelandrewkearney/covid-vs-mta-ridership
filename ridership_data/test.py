


difflist = [10, 100, 600, 700, 705]

buffer = 10
boundaries = []

mini = min(difflist)
maxi = max(difflist)

while difflist:
    start = end = difflist.pop()
    while difflist and start - difflist[-1] <= buffer*2:
        start = difflist.pop()
    boundaries.append((max(start-buffer, mini), start, end, min(end+buffer, maxi)))

print(boundaries)

#for start, end in boundaries: