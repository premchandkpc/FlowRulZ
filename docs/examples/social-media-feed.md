# Social Media & Feed

Feed generation, notifications, content moderation, recommendations.

## 1. Feed Generation

### Flow DSL

```
version 1

flow FeedGeneration

service user
    type grpc
    address user:50051

service graph
    type grpc
    address graph:50051

service content
    type grpc
    address content:50051

service ranking
    type http
    url https://ml.internal/rank

service cache
    type redis
    connection redis:6379

service notifications
    type kafka
    brokers kafka:9092
    topic feed-events

timeout 5s

workflow

Start

-> user.GetPreferences

-> graph.GetFollowedContent

-> content.FetchRecent

-> ranking.RankFeed

-> cache.StoreFeed

-> Return {feed: ranking.ranked_feed, count: ranking.item_count}
```

## 2. Content Moderation

### Flow DSL

```
version 1

flow ContentModeration

service content
    type grpc
    address content:50051

service ai-moderation
    type http
    url https://ai.internal/moderate

service human-review
    type grpc
    address humanreview:50051

service notifications
    type kafka
    brokers kafka:9092
    topic moderation-events

constants
    AUTO_APPROVE_THRESHOLD float = 0.95
    AUTO_REJECT_THRESHOLD float = 0.10

timeout 10s

workflow

Start

-> content.GetContent

-> ai-moderation.Analyze

-> if ai-moderation.confidence > AUTO_APPROVE_THRESHOLD
    then
        -> content.Approve
        -> notifications.SendApproved
    else
        -> if ai-moderation.confidence < AUTO_REJECT_THRESHOLD
            then
                -> content.Reject
                -> notifications.SendRejected
            else
                -> human-review.QueueForReview
                -> notifications.SendPendingReview

-> Return {status: content.status, confidence: ai-moderation.confidence}
```

## 3. Notification System

### Flow DSL

```
version 1

flow NotificationSystem

service user
    type grpc
    address user:50051

service preference
    type grpc
    address preference:50051

service push
    type http
    url https://push.internal/send

service email
    type http
    url https://email.internal/send

service sms
    type http
    url https://sms.internal/send

service in-app
    type grpc
    address inapp:50051

service rate-limiter
    type grpc
    address ratelimit:50051

timeout 5s

workflow

Start

-> user.GetNotificationPrefs

-> preference.GetChannels

-> rate-limiter.CheckLimit

-> if rate-limiter.allowed == false
    then
        -> Return {status: "rate_limited"}

-> if preference.push_enabled == true
    then
        -> push.Send

-> if preference.email_enabled == true
    then
        -> email.Send

-> if preference.sms_enabled == true
    then
        -> sms.Send

-> in-app.Send

-> Return {channels: preference.channels, status: "sent"}
```

## 4. User Recommendations

### Flow DSL

```
version 1

flow UserRecommendations

service user
    type grpc
    address user:50051

service graph
    type grpc
    address graph:50051

service ml-recommendations
    type http
    url https://ml.internal/recommend

service content
    type grpc
    address content:50051

service notifications
    type kafka
    brokers kafka:9092
    topic recommendation-events

timeout 10s

workflow

Start

-> user.GetProfile

-> graph.GetSocialGraph

-> ml-recommendations.Recommend

-> content.FilterContent

-> notifications.SendRecommendations

-> Return {recommendations: ml-recommendations.items, count: ml-recommendations.count}
```

## 5. Content Publishing

### Flow DSL

```
version 1

flow ContentPublishing

service auth
    type grpc
    address auth:50051

service content
    type grpc
    address content:50051

service media
    type http
    url https://media.internal/process
    method POST

service moderation
    type grpc
    address moderation:50051

service feed
    type grpc
    address feed:50051

service notifications
    type kafka
    brokers kafka:9092
    topic publishing-events

timeout 30s

workflow

Start

-> auth.ValidateSession

-> content.CreateDraft

-> media.ProcessMedia

-> moderation.CheckContent

-> if moderation.approved == true
    then
        -> content.Publish
        -> feed.UpdateFollowers
        -> notifications.SendPublished
        -> Return {content_id: content.id, status: "published"}
    else
        -> content.Reject
        -> notifications.SendRejected
        -> Return {status: "rejected", reason: moderation.reason}
```
