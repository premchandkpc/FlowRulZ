# ML Pipeline

Training, inference, feature engineering, model serving.

## 1. Model Training Pipeline

### Flow DSL

```
version 1

flow ModelTraining

service data-source
    type grpc
    address datasource:50051

service feature-store
    type grpc
    address features:50051

service trainer
    type http
    url https://ml.internal/train
    method POST

service evaluator
    type http
    url https://ml.internal/evaluate

service registry
    type http
    url https://ml.internal/register
    method POST

service deployer
    type http
    url https://ml.internal/deploy
    method POST

service notifications
    type kafka
    brokers kafka:9092
    topic ml-events

constants
    MIN_ACCURACY float = 0.85
    TRAINING_TIMEOUT string = "2h"

timeout 120m

workflow

Start

-> data-source.ExtractTrainingData

-> feature-store.ComputeFeatures

-> trainer.TrainModel

-> evaluator.EvaluateModel

-> if evaluator.accuracy >= MIN_ACCURACY
    then
        -> registry.RegisterModel
        -> deployer.DeployCanary
        -> notifications.SendModelDeployed
        -> Return {model_id: registry.model_id, accuracy: evaluator.accuracy, status: "deployed"}
    else
        -> notifications.SendModelRejected
        -> Return {accuracy: evaluator.accuracy, status: "rejected", threshold: MIN_ACCURACY}

onError
    Default
        -> notifications.SendTrainingFailed
```

## 2. Real-Time Inference

### Bytecode DSL

```
schema:{!user_id:string,!features:object} | n:feature-engineering | n:model-predict | g:confidence>0.8 n:act-on-prediction | g:confidence<=0.8 n:queue-for-review | e:inference-logged
```

### Flow DSL

```
version 1

flow RealTimeInference

service feature-engineering
    type grpc
    address features:50051

service model-serving
    type http
    url https://ml.internal/predict
    method POST

service feature-store
    type grpc
    address featurestore:50051

service actions
    type grpc
    address actions:50051

service monitoring
    type kafka
    brokers kafka:9092
    topic inference-events

constants
    CONFIDENCE_THRESHOLD float = 0.8

timeout 500ms

workflow

Start

-> feature-engineering.TransformInput

-> model-serving.Predict

-> if model-serving.confidence >= CONFIDENCE_THRESHOLD
    then
        -> actions.ExecuteAction
        -> monitoring.LogPrediction
    else
        -> monitoring.LogLowConfidence
        -> actions.QueueForHumanReview

-> Return {prediction: model-serving.result, confidence: model-serving.confidence}
```

## 3. A/B Testing Pipeline

### Flow DSL

```
version 1

flow ABTesting

service traffic-splitter
    type grpc
    address splitter:50051

service model-a
    type http
    url https://ml-a.internal/predict

service model-b
    type http
    url https://ml-b.internal/predict

service metrics
    type grpc
    address metrics:50051

service optimizer
    type grpc
    address optimizer:50051

service notifications
    type kafka
    brokers kafka:9092
    topic ab-test-events

constants
    MIN_SAMPLE_SIZE int = 10000
    CONFIDENCE_LEVEL float = 0.95

workflow

Start

-> traffic-splitter.AssignVariant

-> if traffic-splitter.variant == "A"
    then
        -> model-a.Predict
    else
        -> model-b.Predict

-> metrics.RecordOutcome

-> if metrics.sample_size >= MIN_SAMPLE_SIZE
    then
        -> optimizer.CalculateSignificance
        -> if optimizer.is_significant == true
            then
                -> optimizer.RecommendWinner
                -> notifications.SendABTestResult
            else
                -> notifications.SendInsufficientData

-> Return {variant: traffic-splitter.variant, prediction: model.result}
```

## 4. Feature Engineering Pipeline

### Flow DSL

```
version 1

flow FeatureEngineering

service raw-data
    type grpc
    address rawdata:50051

service feature-compute
    type grpc
    address compute:50051

service feature-store
    type grpc
    address featurestore:50051

service validation
    type grpc
    address validation:50051

service monitoring
    type kafka
    brokers kafka:9092
    topic feature-events

timeout 30s

workflow

Start

-> raw-data.Fetch

-> feature-compute.ComputeFeatures

-> validation.ValidateFeatures

-> if validation.drift_detected == true
    then
        -> monitoring.SendDriftAlert
        -> feature-compute.RecomputeWithUpdatedSchema

-> feature-store.Store

-> monitoring.TrackFeatureStats

-> Return {feature_set: feature-store.feature_set_id, features: feature-store.feature_count}
```

## 5. Model Monitoring

### Flow DSL

```
version 1

flow ModelMonitoring

service prediction-log
    type grpc
    address predictions:50051

service ground-truth
    type grpc
    address groundtruth:50051

service metrics
    type grpc
    address metrics:50051

service alerting
    type http
    url https://alerts.internal/send

service retraining
    type http
    url https://ml.internal/retrain

constants
    DRIFT_THRESHOLD float = 0.05
    PERFORMANCE_THRESHOLD float = 0.80

trigger cron
    schedule "0 */6 * * *"

workflow

Start

-> prediction-log.GetRecentPredictions

-> ground-truth.GetActualOutcomes

-> metrics.CalculatePerformance

-> if metrics.accuracy < PERFORMANCE_THRESHOLD
    then
        -> alerting.SendPerformanceAlert
        -> retraining.TriggerRetraining
        -> Return {action: "retraining_triggered", accuracy: metrics.accuracy}

-> metrics.CalculateDataDrift

-> if metrics.drift_score > DRIFT_THRESHOLD
    then
        -> alerting.SendDriftAlert
        -> Return {action: "drift_detected", drift: metrics.drift_score}

-> Return {action: "healthy", accuracy: metrics.accuracy, drift: metrics.drift_score}
```
