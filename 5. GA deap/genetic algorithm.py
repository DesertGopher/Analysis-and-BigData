import random
from deap import creator, base, tools


class Vkusnost:
    def __init__(self, min_weight, max_weight, prices, budget, tastiness):
        self.min_weight = min_weight
        self.max_weight = max_weight
        self.prices = prices
        self.budget = budget
        self.tastiness = tastiness

    def total_tastiness(self, individual):
        return sum([t * w for t, w in zip(self.tastiness, individual)])

    def evaluate_with_budget(self, individual):
        total_cost = sum([p * w for p, w in zip(self.prices, individual)])
        if total_cost <= self.budget and all(self.min_weight <= w <= self.max_weight for w in individual):
            return self.total_tastiness(individual)
        else:
            return (0.0,)

    def creator(self):
        creator.create("FitnessMax", base.Fitness, weights=(1.0,))
        creator.create("Individual", list, fitness=creator.FitnessMax)

        toolbox = base.Toolbox()
        toolbox.register("attr_float", random.uniform, self.min_weight, self.max_weight)
        toolbox.register(
            "individual", tools.initRepeat, creator.Individual, toolbox.attr_float, n=3
        )
        toolbox.register("population", tools.initRepeat, list, toolbox.individual)
        toolbox.register("evaluate", self.evaluate_with_budget)
        toolbox.register("mate", tools.cxOnePoint)
        toolbox.register("mutate", tools.mutGaussian, mu=0, sigma=1, indpb=0.1)
        toolbox.register("select", tools.selNSGA2)

        population = toolbox.population(n=50)
        hof = tools.HallOfFame(1)

        for ind in population:
            ind.fitness.values = toolbox.evaluate(ind)

        return hof


if __name__ == "__main__":
    vkusno = Vkusnost(min_weight=1, max_weight=10, prices=[100, 400, 140], budget=3000, tastiness=[10, 100, 80])
    best_individual = vkusno.creator()
    print(f"Самое вкусное: {best_individual}")
    print(f"Суммарная вкусность: {best_individual.fitness.values[0]}")
