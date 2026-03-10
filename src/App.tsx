import React, { useState } from 'react';

type Position = 'UTG' | 'MP' | 'CO' | 'BTN' | 'SB' | 'BB';
type Action = 'Open' | 'Call 3-bet' | '4-bet' | 'Defend vs Open';

type RangeKey = `${Position}-${Action}`;

const positions: Position[] = ['UTG', 'MP', 'CO', 'BTN', 'SB', 'BB'];
const actions: Action[] = ['Open', 'Call 3-bet', '4-bet', 'Defend vs Open'];

const allHands: string[] = (() => {
  const ranks = ['A', 'K', 'Q', 'J', 'T', '9', '8', '7', '6', '5', '4', '3', '2'];
  const hands: string[] = [];

  for (let i = 0; i < ranks.length; i++) {
    for (let j = 0; j < ranks.length; j++) {
      const r1 = ranks[i];
      const r2 = ranks[j];
      if (i === j) {
        hands.push(`${r1}${r2}`);
      } else if (i < j) {
        hands.push(`${r1}${r2}s`);
        hands.push(`${r1}${r2}o`);
      }
    }
  }
  return hands;
})();

function buildEmptyRange(): Record<RangeKey, Set<string>> {
  const map: Record<RangeKey, Set<string>> = {} as Record<RangeKey, Set<string>>;
  for (const pos of positions) {
    for (const act of actions) {
      const key: RangeKey = `${pos}-${act}`;
      map[key] = new Set<string>();
    }
  }
  return map;
}

export const App: React.FC = () => {
  const [selectedPosition, setSelectedPosition] = useState<Position>('BTN');
  const [selectedAction, setSelectedAction] = useState<Action>('Open');
  const [ranges, setRanges] = useState<Record<RangeKey, Set<string>>>(buildEmptyRange);
  const [selectedHandForTraining, setSelectedHandForTraining] = useState<string | null>(null);
  const [isInRange, setIsInRange] = useState<boolean | null>(null);

  const currentKey: RangeKey = `${selectedPosition}-${selectedAction}`;
  const currentRange = ranges[currentKey];

  function toggleHandInRange(hand: string) {
    setRanges(prev => {
      const next: Record<RangeKey, Set<string>> = {} as Record<RangeKey, Set<string>>;
      (Object.keys(prev) as RangeKey[]).forEach(key => {
        next[key] = new Set(prev[key]);
      });
      const set = next[currentKey];
      if (set.has(hand)) {
        set.delete(hand);
      } else {
        set.add(hand);
      }
      return next;
    });
  }

  function startTraining() {
    const rangeHands = Array.from(currentRange);
    if (rangeHands.length === 0) {
      setSelectedHandForTraining(null);
      setIsInRange(null);
      return;
    }
    const randomIndex = Math.floor(Math.random() * allHands.length);
    const hand = allHands[randomIndex];
    setSelectedHandForTraining(hand);
    setIsInRange(null);
  }

  function answer(isIncludedGuess: boolean) {
    if (!selectedHandForTraining) return;
    const actuallyInRange = currentRange.has(selectedHandForTraining);
    setIsInRange(isIncludedGuess === actuallyInRange);
  }

  return (
    <div className="app">
      <header className="app-header">
        <h1>Poker Range Trainer</h1>
        <p>Build your preflop ranges and quiz yourself.</p>
      </header>

      <main className="app-main">
        <section className="panel">
          <h2>1. Configure spot</h2>
          <div className="controls-row">
            <div className="control">
              <label>Position</label>
              <select
                value={selectedPosition}
                onChange={e => setSelectedPosition(e.target.value as Position)}
              >
                {positions.map(p => (
                  <option key={p} value={p}>
                    {p}
                  </option>
                ))}
              </select>
            </div>
            <div className="control">
              <label>Action</label>
              <select
                value={selectedAction}
                onChange={e => setSelectedAction(e.target.value as Action)}
              >
                {actions.map(a => (
                  <option key={a} value={a}>
                    {a}
                  </option>
                ))}
              </select>
            </div>
          </div>
        </section>

        <section className="panel">
          <h2>2. Build range</h2>
          <div className="grid">
            {allHands.map(hand => {
              const active = currentRange.has(hand);
              return (
                <button
                  key={hand}
                  className={`grid-cell ${active ? 'grid-cell--active' : ''}`}
                  onClick={() => toggleHandInRange(hand)}
                  type="button"
                >
                  {hand}
                </button>
              );
            })}
          </div>
        </section>

        <section className="panel">
          <h2>3. Train</h2>
          <p>
            Trainer uses the currently selected position + action. First build a range, then start a
            quiz.
          </p>
          <button className="primary" type="button" onClick={startTraining}>
            New hand
          </button>

          {selectedHandForTraining && (
            <div className="trainer">
              <div className="trainer-hand">{selectedHandForTraining}</div>
              <div className="trainer-actions">
                <button type="button" onClick={() => answer(true)}>
                  In my range
                </button>
                <button type="button" onClick={() => answer(false)}>
                  Not in my range
                </button>
              </div>
              {isInRange !== null && (
                <div className={`trainer-result ${isInRange ? 'trainer-result--good' : 'trainer-result--bad'}`}>
                  {isInRange ? 'Correct!' : 'Incorrect'}
                </div>
              )}
            </div>
          )}
        </section>
      </main>
    </div>
  );
};
