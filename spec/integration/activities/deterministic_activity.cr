class DeterministicActivity
  include Temporalio::Activity

  activity_name "DeterministicActivity"

  def execute(value : Int64) : Int64
    # Always returns the same output for the same input
    value * 2
  end
end
