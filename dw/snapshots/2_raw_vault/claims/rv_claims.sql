{% snapshot rv_claims__claims_data %}

    {{ build_snapshot_model(this.name) }}

{% endsnapshot %}

{% snapshot rv_claims__claims %}

    {{ build_snapshot_model(this.name) }}

{% endsnapshot %}

{% snapshot rv_claims__claims_eras %}

    {{ build_snapshot_model(this.name) }}

{% endsnapshot %}

{% snapshot rv_claims__claims_payments %}

    {{ build_snapshot_model(this.name) }}

{% endsnapshot %}

{% snapshot rv_claims__claims_transactions %}

    {{ build_snapshot_model(this.name) }}

{% endsnapshot %}

{% snapshot rv_claims__claims_submitted %}

    {{ build_snapshot_model(this.name) }}

{% endsnapshot %}
