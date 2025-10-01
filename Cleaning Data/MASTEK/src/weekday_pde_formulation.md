# PDE Formulation for Weekday Momentum Dynamics

Let $\Phi(t, f, p, L)$ denote the joint probability density of observing a streak
of length $L$ with frequency $f$ (counts per observation window) when the weekday
index is $t$ and the associated single-day percentage change is $p$. A parsimonious
continuity equation that couples these variables is:

$$
\frac{\partial \Phi}{\partial t}
+ \frac{\partial}{\partial L}\left( (\alpha_1 p - \alpha_2 L)\,\Phi \right)
+ \frac{\partial}{\partial p}\left( (\beta_0 + \beta_1 p)\,\Phi \right)
- D_f\,\frac{\partial^2 \Phi}{\partial f^2}
= S(t, f, p, L).
$$

Where:

- $t$ indexes trading weekdays (0 for Monday through 4 for Friday) but can be
  treated as continuous for analytical convenience.
- $f$ is the observed frequency of a streak length within a rolling window.
- $p$ is the single-day percentage change for that weekday occurrence.
- $L$ is the integer streak length (consecutive weeks of the same direction).
- $\alpha_1$ captures how stronger positive moves extend streaks; $\alpha_2$ enforces
  mean reversion in streak length.
- $\beta_0, \beta_1$ describe drift in the daily return distribution.
- $D_f$ is a diffusion coefficient modeling dispersion of observed frequencies.
- $S(t, f, p, L)$ is an optional source term for exogenous shocks (earnings, events).

Boundary and initial conditions must be supplied based on empirical estimates,
for example: $\Phi(0, f, p, L)$ fitted from historical data, reflecting the
observed Monday distribution at the start of the sample.
