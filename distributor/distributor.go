package distributor

import (
	"go.uber.org/zap"
	"slices"
	"sync/atomic"
)

// subscriber is the internal representation of a subscriber
type subscriber struct {
	id             uint64 // used to identify subscriber
	lastSeenDataID uint64
	reassignOnFail bool // true = if the subscriber fails to deliver the data (unsubscribe func called with false) the data should be re-assigned to the next subscriber.
	assignedData   *DataWithID
	dataOutputChan chan<- DataWithID
}

// lostSubscriberInfo will be passed to the main loop to handle unsubscription
type lostSubscriberInfo struct {
	id              uint64
	deliverySuccess bool
}

type Distributor struct {
	lastData           atomic.Pointer[DataWithID]
	dataInputChan      chan Data
	lastSubscriberID   atomic.Uint64 // used only by "subscribe" to assign ids
	newSubscriberChan  chan subscriber
	lostSubscriberChan chan lostSubscriberInfo

	logger *zap.Logger

	closeChan chan interface{}
	closed    atomic.Bool
}

// NewDistributor creates a new distributor and starts its main loop in a goroutine. To stop the main loop call Close().
func NewDistributor(logger *zap.Logger) *Distributor {
	inputChan := make(chan Data, 5)
	newSubscriberChan := make(chan subscriber, 2)
	lostSubscriberChan := make(chan lostSubscriberInfo, 2)
	closeChan := make(chan interface{})
	d := Distributor{
		dataInputChan:      inputChan,
		newSubscriberChan:  newSubscriberChan,
		lostSubscriberChan: lostSubscriberChan,
		logger:             logger,
		closeChan:          closeChan,
	}
	go d.run()
	return &d
}

func assignNextAvailable(subscribers []subscriber, data *DataWithID) (int, *subscriber) {
	// start from the beginning
	for idx, sub := range subscribers {
		if sub.assignedData == nil {
			subscribers[idx].assignedData = data
			sub.dataOutputChan <- *data
			return idx, &sub
		}
	}
	return -1, nil
}

// run executes the main loop of the Distributor
func (d *Distributor) run() {
	// We just put all magic to a single goroutine, so we don't have to deal with weird locking mechanisms...
	var subscribers []subscriber
	var dataID uint64 // we have to assign dataID here, otherwise it would be possible that they arrive swapped so they won't be incremental, which would mess everything up

	d.logger.Debug("Distributor main loop start...")
	for {
		lgr := d.logger.With(zap.Int("len_subscribers", len(subscribers)))
		select {
		case newData := <-d.dataInputChan:
			lastData := d.lastData.Load()
			if lastData != nil && lastData.Data == newData {
				lgr.Debug("Received new data, but it is the same as last data... skipping")
				continue
			}

			dataID++
			newDataWithId := DataWithID{Data: newData, ID: dataID}
			d.lastData.Store(&newDataWithId) // only written by this thread, so we are fine from the write-after-free bugs
			placeInQueue, assignee := assignNextAvailable(subscribers, &newDataWithId)
			if placeInQueue != -1 && assignee != nil {
				lgr.Info("Assigned new data to a subscriber",
					zap.Uint64("dataID", newDataWithId.ID),
					zap.Uint64("assigned_to_subscriber_id", assignee.id),
					zap.Int("placeInQueue", placeInQueue),
				)
			} else {
				lgr.Info("Could not assign new data to any subscriber", zap.Uint64("dataID", newDataWithId.ID))
			}
		case sub := <-d.newSubscriberChan:
			sub.reassignOnFail = true // reassign by default

			currentLastData := d.lastData.Load()
			if currentLastData != nil {
				if sub.lastSeenDataID < currentLastData.ID {
					// send the last data...
					sub.dataOutputChan <- *currentLastData
					sub.reassignOnFail = false // stale data does not need to be re-assigned...

					lgr.Debug("Subscriber is out-of-date. Sending the latest data instantly...", zap.Uint64("sub.id", sub.id))
				}
			}

			// the subscriber is up-to-date, so just assign them to the "queue"
			subscribers = append(subscribers, sub) // append to the end

			lgr.Info("New subscriber accepted", zap.Uint64("sub.id", sub.id), zap.Bool("sub.reassignOnFail", sub.reassignOnFail)) // the length here may be out-of-date by one

		case unsub := <-d.lostSubscriberChan:

			idx := slices.IndexFunc(subscribers, func(s subscriber) bool { return s.id == unsub.id })
			if idx == -1 {
				lgr.Warn("Could not match lost subscriber ID to any current subscriber ID", zap.Uint64("unsub.id", unsub.id))
				// nothing to do...
				continue
			}
			lgr.Debug("Identified lost subscriber", zap.Uint64("unsub.id", unsub.id), zap.Int("placeInQueue", idx))

			// Pop subscriber from list
			sub := subscribers[idx]
			subscribers = slices.Delete(subscribers, idx, idx+1)

			// Check if it had assigned data that needs to be re-assigned...
			if !unsub.deliverySuccess && sub.reassignOnFail && sub.assignedData != nil {
				placeInQueue, assignee := assignNextAvailable(subscribers, sub.assignedData)
				if placeInQueue != -1 && assignee != nil {
					lgr.Info("Re-assigned undelivered data to a subscriber",
						zap.Uint64("unsub.id", unsub.id),
						zap.Uint64("dataID", sub.assignedData.ID),
						zap.Uint64("assigned_to_subscriber_id", assignee.id),
						zap.Int("placeInQueue", placeInQueue),
					)
				} else {
					lgr.Info("Could not assign undelivered data to any other subscriber", zap.Uint64("unsub.id", unsub.id), zap.Uint64("dataID", sub.assignedData.ID))
				}
			} else {
				lgr.Debug("Data (if there was any) was not re-assigned",
					zap.Uint64("unsub.id", unsub.id),
					zap.Bool("unsub.deliverySuccess", unsub.deliverySuccess),
					zap.Bool("sub.reassignOnFail", sub.reassignOnFail),
					zap.Bool("had_assigned_data", sub.assignedData != nil),
				)
			}

			// close the channel
			close(sub.dataOutputChan)
		case <-d.closeChan:
			// cleanup
			lgr.Debug("Shutting down main loop...")
			close(d.dataInputChan)
			close(d.newSubscriberChan)
			close(d.lostSubscriberChan)
			for _, sub := range subscribers {
				close(sub.dataOutputChan)
			}
			return // exit the main loop
		}
	}
}

// Subscribe creates a new subscription for data events. If the lastId is smaller than the currently recorded last data, the subscriber will immediately receive the current last data, without queueing up.
// This function returns a channel where the new data will be received. After data is received, no more data will be ever sent on the channel, but it should not be closed, this will be handled by the distributor!
// The second return value is an "unsubscribe" function. It always has to be called when the subscriber closes either because it delivered the single data assigned to it, or because the connection is being closed for some reason.
// The only parameter to this function should indicate if the subscriber were able to successfully deliver the single data assigned to it.
// If the subscriber never received a data (closing the connection for some other reason) this should be false.
// If the subscriber received the single message, but were unable to deliver, this should be false.
// If the subscriber received the single message and did it's best to deliver it, then the parameter should be true.
// This function should never be called more than once!!
func (d *Distributor) Subscribe(lastID uint64) (<-chan DataWithID, func(bool)) {
	if d.closed.Load() {
		panic("distributor is being closed") // should be avoided...
	}

	id := d.lastSubscriberID.Add(1) // This does not need to be incremental, just unique
	d.logger.Debug("Notification of new subscriber request", zap.Uint64("subscriberID", id), zap.Uint64("lastID", lastID))

	dataChan := make(chan DataWithID, 1)
	d.newSubscriberChan <- subscriber{
		id:             id,     // will be used to refer this subscriber
		lastSeenDataID: lastID, // can be zero if this is the first time the subscriber is subscribing
		assignedData:   nil,    // will be set by the main loop thingy
		dataOutputChan: dataChan,
	}

	unsubFunction := func(deliverySuccess bool) {
		closed := d.closed.Load()
		d.logger.Debug("Notification of loosing a subscriber", zap.Uint64("subscriberID", id), zap.Bool("deliverySuccess", deliverySuccess), zap.Bool("distributor_closed", closed))
		if closed {
			// do nothing if the Distributor have been closed
			return
		}
		d.lostSubscriberChan <- lostSubscriberInfo{
			id:              id,
			deliverySuccess: deliverySuccess,
		}
	}

	return dataChan, unsubFunction
}

// Distribute initiates processing of new data. New data will be assigned a new ID and will be delivered to the longest waiting ready subscriber
// If there are no ready subscribers, the data won't be instantly delivered
// Also updates the LastData.
func (d *Distributor) Distribute(data Data) {
	closed := d.closed.Load()
	d.logger.Debug("Notification of new data", zap.Bool("distributor_closed", closed))
	if !closed {
		d.dataInputChan <- data
	}
}

// GetLastData Gets the LastData. This is the last data element distributed by Distribute.
func (d *Distributor) GetLastData() DataWithID {
	lastData := d.lastData.Load()
	if lastData == nil {
		return DataWithID{} // return default state
	} else {
		return *lastData
	}
}

// Close shuts down the distributor. After calling this function, the distributor should not be used at all... It is fine to call pending unsubscribe functions, but that's about it.
// All open channels will be closed immediately. (make use of the ok return value of the channels to check if they are closed...)
// Make sure that no new Subscribe calls will be made to this distributor as it will result in panic! Distribute will just fail silently.
// GetLastData will still be usable tho...
func (d *Distributor) Close() {
	d.closed.Store(true)
	close(d.closeChan)
}
