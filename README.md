# patelco

Analyze financial transactions with an eye toward improving personal/
household budgets by reducing unnecessary expenses. Initial
implementation focuses on data from Patelco Credit Union in CSV format.

One goal of this project is to consume data in any of several
industry-standard formats from arbitrary financial institutions,
including Open Financial Exchange Format (\*.ofx); Quicken WebConnect
(\*.qfx); and Quickbooks (\*.qbo).


#### Text-based User Interface
Some human interaction will be necessary to create groups of
transactions over which aggregations can be applied. As of now, the
best approach to adding an interactive interface seems to be the core
Python library `curses`. The major disadvantage of using `curses` is
that it's not supported on Windows. However the popular [UniCurses](https://pypi.org/project/UniCurses/)
library is cross-platform and presumably provides a similar API to
`curses`.
