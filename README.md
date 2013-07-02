# Vector

Vector is a tool that augments your auto-scaling groups. The two
features currently offered are Predictive Scaling and Flexible Down
Scaling.

## Predictive scaling

Auto Scaling groups do a good job of responding to current
load conditions, but if you have a predictable load pattern,
it can be nice to scale up your servers a little bit *early*.
Some reasons you might want to do that are:

 * If it takes several minutes for an instance to fully boot
   and ready itself for requests.
 * If you have very serious (but predictable) spikes,
   it's nice to have the capacity in place before the spike
   starts.
 * To give yourself a buffer of time if AWS APIs start
   throwing errors. If scaling up is going to fail, you'd
   rather it start failing with a little bit of time before
   you actually need the capacity so you can begin evasive maneuvers.

Vector examines your existing CloudWatch alarms tied to your Auto
Scaling groups, and checks those metrics in the past. It does this by
comparing the "Load" of the metric in the past versus the present. The
"Load" of a metric is defined as the metric_value *
number_of_current_instances.

Vector will first check the metric load right now and the metric load in
the past, based on the lookback window. If those are within a threshold,
we assume that the future will mirror the past and continue.

Vector then retrieves the load of the metric from the past plus the
lookahead window. That load is divided by the current number of
instances to get the "predicted load". That value is compared against
the alarm, and if it would send the alarm into ALARM state, it triggers
the scaleup policy.

This method will only work if multiplying your metric by # of instances
to come up with a representation of load across the group makes sense.
For things like CPUUtilization this is true. Other metric types should
be thought through.

If you use Predictive Scaling, you probably also want to use Flexible
Down Scaling (below) so that after scaling up in prediction of load,
your scaledown policy doesn't quickly undo Vector's hard work. You
probably want to set `up-to-down-cooldown` to be close to the size of
your `lookahead-window`.

## Flexible Down Scaling

Auto Scaling Groups support the concept of "cooldown periods" - a window
of time after a scaling activity where no other activities should take
place. This is to give the group a chance to settle into the new
configuration before deciding whether another action is required.

However, Auto Scaling Groups only support specifying the cooldown period
*after* a certain activity - you can say "After a scale up, wait 5
minutes before doing anything else, and after a scale down, wait 15
minutes." What you can't do is say "After a scale up, wait 5 minutes for
another scale up, and 40 minutes for a scale down."

Vector lets you add custom up-to-down and down-to-down cooldown periods.
You create your policies and alarms in your Auto Scaling Groups like
normal, and then *disable* the alarm you want a custom cooldown period
applied to. Then you tell Vector what cooldown periods to use, and he
does the rest.

## Requirements

 * Auto Scaling groups must have the GroupInServiceInstances metric
   enabled.
 * Auto Scaling groups must have at least one scaling policy with a
   positive adjustment, and that policy must have at least one
   CloudWatch alarm with a CPUUtilization metric.

## Installation

```bash
$ gem install vector
```

## Usage

Typically vector will be invoked via cron periodically (every 10 minutes
is a good choice.)

```
Usage: vector [options]
DURATION can look like 60s, 1m, 5h, 7d, 1w
        --timezone TIMEZONE          Timezone to use for date calculations (like America/Denver) (default: system timezone)
        --region REGION              AWS region to operate in (default: us-east-1)
        --groups group1,group2       A list of Auto Scaling Groups to evaluate
        --fleet fleet                An AWS ASG Fleet (instead of specifying a list of Groups
    -v, --[no-]verbose               Run verbosely

Predictive Scaling Options
        --[no-]ps                    Enable Predictive Scaling
        --ps-lookback-windows DURATION,DURATION
                                     List of lookback windows
        --ps-lookahead-window DURATION
                                     Lookahead window
        --ps-valid-threshold FLOAT   A number from 0.0 - 1.0 specifying how closely previous load must match current load for Predictive Scaling to take effect
        --ps-valid-period DURATION   The period to use when doing the threshold check

Flexible Down Scaling Options
        --[no-]fds                   Enable Flexible Down Scaling
        --fds-up-to-down-cooldown DURATION
                                     The cooldown period between up and down scale events
        --fds-down-to-down-cooldown DURATION
                                     The cooldown period between down and down scale events
```

# Questions

### Why not just predictively scale based on the past DesiredInstances?

If we don't look at the actual utilization and just look
at how many instances we were running in the past, we will end up
scaling earlier and earlier, and will never re-adjust and not scale up
if load patterns change, and we don't need so much capacity.

### What about high availability? What if the box Vector is running on dies?

Luckily Vector is just providing optimizations - the critical component
of scaling up based on demand is still provided by the normal Auto
Scaling service. If Vector does not run, you just don't get the
predictive scaling and down scaling.

