_temp file, delete before any PR gets merged please_.

# Issue 77

**Please note - this is my unreviewed initial work only, ignore, supersede or delete as needed.**

task: https://app.zenhub.com/workspaces/features-squad-61b72275ac896c0010a1b00b/issues/gss-cogs/csvw-check/77

This task got deprioritised in flight, these are just some notes on work done thus far in case it's of use to someone in the future.


## What's done

* Restructured BDD statements to be a lot simpler and follow more of an OOP approach.
* Http mocking logic/approach.

**NOTE** - I've rewritten the phrasing of the steps to make more sense, but (a) only the one I was working on (the one tagged with `@runme`) and (b) those steps are generated from a script, if you agree with my rephrasing you'll need to update that script accordingly. I'd personally do this last, in case more tweaks to step phrasing is required.

## What needs doing next

* Complete method to read fixtures into the mock response body.
* Complete method to read provided (in a step) headers into the mock response.


## And then ....

.... you can do the actual task.

The above work is literally just setting up a sensible way to mock http responses, once that is working you should be able to address the three `@ignoreme` tagged features and make the necessary code changes.