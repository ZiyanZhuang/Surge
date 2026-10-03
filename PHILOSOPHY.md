# Surge Mode: Why We Are Here

> “Men make their own history, but they do not make it just as they please; they do not make it under circumstances chosen by themselves, but under circumstances directly encountered, given and transmitted from the past.”
>
> — Karl Marx, *The Eighteenth Brumaire of Louis Bonaparte*

## From the human question

Every serious project eventually meets a question that is larger than its implementation: when a new power comes into our hands, what will we do with the reach it gives us?

Large language models bring quicker drafting, cheaper search, and more accessible programming. That is the visible part. The deeper change may be the radius of a person’s action. One person can read more, test more possibilities, and carry a difficult question further. A small team can take work that once required a much larger organization, break it apart, arrange it again, and make a beginning.

That possibility is exhilarating. It is also dangerous. Anything that magnifies action can magnify carelessness, prejudice, waste, and error. We did not begin with the question, “How many Agents can we spawn?” We began with a quieter and harder question:

> How can a person use a new productive force to do real work—and know when to stop?

Surge Mode is one engineering answer. It is small. It is unfinished. It does not want to be merely a polished demonstration.

## I. Why “Surge”

“Surge” is the name of the project and a key to its argument. It points first to the accumulation of productive forces: navigation, energy, materials, machines, and systems of knowledge gather over time, and at certain thresholds they alter the scale of action and the shape of social organization. It also describes a human situation. No individual commands the sea, yet a person can read the current, repair a boat, plan a passage, and take hold of the rudder. Finally, it names an engineering discipline. A surge has a crest, a trough, and a direction; a system needs stages, gates, budgets, and evidence if that force is to become deliberate progress.

The four internal stages—**Scout, Deepen, Verify, Synthesize**—are therefore called waves. The word carries a practical meaning. A problem first comes into view, effort gathers around its important parts, a conclusion survives verification, and only then does it travel into synthesis. When evidence is missing, a budget is spent, or risk crosses a boundary, the wave closes. The name and the architecture speak to one another.

## II. Historical transitions in productive force

We often tell history in a few strokes. Maritime exchange widened the horizon of trade. Ships, ports, navigational knowledge, contracts, credit, and insurance developed around that wider horizon; goods, capital, people, and disease all moved with greater speed.

Agricultural surplus and land relations supported early administrative states. Slavery, tribute, tenancy, and corvée organized labor and the extraction of wealth in different ways. Census, storage, roads, taxation, and armies brought scattered populations into larger political scales. State capacity appeared as mobilization, record-keeping, and provision—and also as coercion, war, and inequality.

Iron and steel changed tools, weapons, fortifications, and transport. The mobility of cavalry depended on horses, roads, fodder, manufacture, training, and supply before it could become battlefield power; personal valor was drawn into formations, logistics, and command. Gunpowder altered the distance of attack, the shape of fortifications, and the finances of war. The important change lay in the system of coordination. A weapon was one part of it.

Steam drew energy partly away from human muscle, animal power, and local water flow. Factories, railways, and steamships acquired new speed. The factory brought machines, division of labor, standard time, and urbanization, while also shaping wage labor, capital accumulation, and new class relations. Productivity rose together with discipline, environmental cost, and social conflict.

These developments appeared in different regions and in different orders; they coexisted for long periods. History offers no single staircase for every society. It does show, however, that a productive force reaches beyond the tool itself. It reorganizes cooperation, scale, authority, and responsibility: who may decide, how far a decision travels, how quickly an error spreads, and which matters must remain in human hands.

In the 1859 preface to *A Contribution to the Critique of Political Economy*, Marx described the tension that can arise when productive forces meet inherited relations. Braudel’s work on maritime worlds and long duration, and Weber’s analyses of economic organization, administration, and modern institutions, offer other ways to see how technique becomes a durable social force only when it is carried by institutions and daily practice. We take these works as lenses, not formulas. A growing capability needs forms of cooperation, institutional arrangements, and boundaries of responsibility capable of carrying it.

This is what “surge” means to us: historical pressure produced by accumulated capability, the situation of a person standing within that pressure, and an engineering attempt to give it a vessel through scheduling, evidence, and stopping conditions. We can learn the current, repair the boat, place ballast in its hold, and keep judgment at the helm.

## III. The rider and the commander

We borrow these two figures without glorifying war and without pretending that Agents are armies. What we borrow is the question of responsibility: who holds the reins, who sees the terrain, who decides that the charge should end?

When one person works with a model, they are first a **rider**. The Agent is a new productive instrument: it reads a pile of material, tests a piece of code, searches for an opening in a problem, and offers a path worth considering. The rider does not surrender the reins. With another horse, they can travel farther.

When the work grows beyond one person, the person becomes a **commander**—not in the theatrical sense of issuing orders, but in the demanding sense of holding a whole situation in view. One Agent scouts. Another goes deeper. A third looks for the failure. A fourth gathers what survives. The commander sets the aim, distributes scarce resources, decides when the evidence is enough, and accepts responsibility for the result.

This is why Surge Mode has no devotion to an unlimited swarm. One more Agent is not automatically one more insight. More calls are not automatically closer to truth. The real questions are less glamorous and more important: what should happen in parallel, what must wait, which evidence can be handed on, which conclusion must be checked again, and when further effort has become noise.

A good commander does not put every soldier in the field simply because there are soldiers to spare. A good engineer does not turn every problem into a concurrent request merely because an API is cheap.

## IV. What we hope to bring to our time

We do not imagine that a scheduler will change history. What we can do is smaller, and therefore more concrete.

We want to loosen the old bind in which a person must personally carry every part of a difficult task—but not loosen the person from judgment or responsibility. We want a small team to be able to separate complex work, give different abilities their proper place, and return every important conclusion to the evidence from which it came.

We are not trying to build a machine that lives in our place. We are trying to build a tool that gives people more reach without quietly taking away their sense of direction.

That is why Surge Mode puts apparently unglamorous things at the center: budgets, dependencies, leases, timeouts, retries, artifacts, evidence references, audit events, and explicit failure states. They are ballast. Without ballast, a boat may leave the shore quickly; the first hard turn can still put it under.

### Four practical principles

**Productivity serves the real question.** Agent count, call volume, and model complexity are means. A code problem, a statistical analysis, and a research question each require their own decomposition, stages, and standards of verification.

**Organization and model capability form one system.** A stronger model may improve a node; dependencies, waves, evidence, and stopping conditions turn local ability into a traceable whole. The `WorkerAdapter` boundary leaves model access explicit while the Host retains concurrency, budgets, state, and recovery.

**Three kinds of fact stay distinct.** Scheduler facts cover dependency order, budget boundaries, and artifact traceability. Model facts cover correctness, format, and stability. System facts cover provider latency, throttling, failures, cost, and capacity. Offline replay, one authorized call, and repeated capacity experiments answer different questions.

**A system must be able to stop.** Budget limits, concurrency caps, timeouts, retry limits, wave gates, evidence references, and at-least-once semantics are operating constraints. People retain responsibility for setting aims, judging risk, and reviewing outcomes.

In code, the hope takes a particular shape:

- **Scout → Deepen → Verify → Synthesize**: see first, then go deeper; propose, then test; synthesize only after something has survived;
- **DAG dependencies**: give collaboration an order and waiting a reason, rather than letting every voice arrive at once;
- **RouteProfile and StagePolicy**: send different work down routes suited to its needs instead of asking one model to be everything;
- **Artifacts and evidence references**: make a beautiful sentence point back to something that can be checked;
- **Budget reservation and settlement**: admit that every action has a cost;
- **Fail-closed behavior**: when evidence is missing, a response is incomplete, or a boundary is unclear, stop rather than dress uncertainty up as success.

The scheduler is not the tide. The model is not destiny. They are, for now, a boat and a set of oars. The direction still depends on how people ask questions, how they treat facts, and how seriously they take those who will live with the consequences of their decisions.

## V. How an ideal reaches the hand

The Chinese writer Lu Xun once wrote: “Give off whatever heat you have; give off whatever light you have. Like a firefly, it may still give a little light in the darkness.” The sentence endures because it does not make hope grand before making it practical. Do what you can do. Say what you can say. Do not wait for the world to become good before you begin to contribute to it.

The ideal of Surge Mode is not a declaration that we already possess the future. We are willing to name what has not been proved: this is a single-host SQLite core, not a distributed cluster; one live adapter call is not a capacity claim; an offline replay is not a model-capability claim; a requested `max_tokens` value is not necessarily a physical upstream limit.

Honesty does not weaken an ideal. It keeps the ideal from having to survive on exaggeration.

Open source, for us, means more than publishing code. It means publishing boundaries, failures, evidence, and the path by which a judgment was reached, so that someone else can continue the work rather than merely trust a finished story. William Morris placed usefulness and beauty in the same sentence. A tool worth keeping should do something real and deserve to be trusted. It should have the sharpness of efficiency without losing the scale of a human life.

## VI. What we hope the tide will leave behind

Years from now, large language models may look like one name among many in the history of technology. They may matter more than we expect, or less. We cannot promise the future on its behalf.

We can choose our posture toward it.

When a new power lets a person go farther, we hope the person becomes more careful, not more arrogant. When it lets a team do more, we hope the team does not forget who bears the consequences. When it makes the world move faster, we hope someone will still stop to check the evidence, hear a different voice, and then decide.

That is what we mean by being part of the surge. Not shouting at the crest, but building a boat when the water reaches us. Not handing people over to machines, but helping people work with machines while still recognizing the horizon—and recognizing themselves.

We may not illuminate an entire age. A small project should not promise that.

We can, however, turn the small heat in our hands into something another person can use, question, and improve. If it helps someone finish one task they could not otherwise finish; if it saves a team one avoidable mistake; if it keeps one important judgment from being led astray by a beautiful hallucination—then the tide has already acquired, here, a direction worth leaving behind.

## Reading and intellectual companions

This essay is not a scholarly survey. These works are companions in thought:

- Karl Marx, *The Eighteenth Brumaire of Louis Bonaparte* (1852);
- Karl Marx, *A Contribution to the Critique of Political Economy*, Preface (1859);
- Hannah Arendt, *The Human Condition* (1958), especially her account of labor, work, action, and the human world built between people;
- Fernand Braudel, *The Mediterranean and the Mediterranean World in the Age of Philip II* (1949);
- Max Weber, *Economy and Society* (1922), especially its analyses of administration, domination, and modern institutions;
- Lu Xun, “Random Thoughts 41,” in *Hot Wind*;
- William Morris, *Hopes and Fears for Art* (1882).

The quoted or paraphrased ideas belong to their authors and editions. They are intellectual references, not endorsements of this project. For a starting point, see the [Marxist research bibliography](http://marxism.cass.cn/jjdd/201311/t20131119_1973728.shtml) and the [Chinese Writers Association text on Lu Xun’s “Random Thoughts 41”](https://www.chinawriter.com.cn/n1/2021/0908/c440988-32221431.html).
