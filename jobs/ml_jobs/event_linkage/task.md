# Main Objective

I want to update the script 3_create_delta_event_tables so that now it takes
into account already created event_series. Basically I want that :

* How to match new offers / unmatched offers :
  * You take in this script another input file : applicative_event_series_offer_link.parquet which contains link between event_series_id and offer_id. Then, try to match only new_offers to already existing event_series_id.
    * If you match them, then create a line in the delta_event_series_offer_link dataframe containing this matched_offer_id with the event_series
    * For all unmatched offers, try to match them between each other. If you find some, then you can create event_series and event_series_offer_link (as it is already done in the current script)
* What to do with "deleted" offers.
* If an offer is "deleted", (present in applicative_event_series_offer_link.parquet but not in the computed_similarities) then do nothing, except if all offers linked to an event_series are deleted. In this case, delete both this event_series and the event_series_offer_link associated with these offers. To delete, you should add lines in the delta tables with the action "remove" inside (you need to create a new value for ActionType class)


Notes : important stuff to know
* We did a bit this same process in the artist_linkage folder, but i think it is a bit overcomplicated. So you should like into it to have the global picture but i would rather like this to be simple as I am explaining right now
* The set of offers on which we have computed similiarities is not exactly the same as the one we have in applicative_event_series_offer_link : we can have new offers or offers previously not linked. And it is possible that offers are "deleted" because the date was before for instance.


Please ask me questions if not clear!
