def generate_random_ranker(data):
    places = {}
    import random

    for place in data:
        places[str(place[0])] = random.randint(0, 1000000)

    return places