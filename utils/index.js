class Utils {
  constructor() {
    this.roundRobinCounter = 0;
    this.NUM_PARTITIONS = 2;
    this.MAX_COUNTER = 1000000;
  }

  selectPartition() {
    const partition = this.roundRobinCounter % this.NUM_PARTITIONS;
    this.roundRobinCounter++;

    // Reset counter periodically to prevent overflow
    if (this.roundRobinCounter >= this.MAX_COUNTER) {
      this.roundRobinCounter = 0;
    }

    return partition;
  }
}

export { Utils };
