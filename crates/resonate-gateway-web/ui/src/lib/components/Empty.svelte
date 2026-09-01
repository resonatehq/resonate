<script lang="ts">
  /** Nothing to show, and why. A title and one explanatory line. */
  let { title, line }: { title: string; line: string } = $props();
</script>

<div class="empty">
  <div class="title">{title}</div>
  <div class="line">{line}</div>
</div>

<style>
  .empty {
    padding: 48px 26px;
    max-width: 52ch;
  }
  .title {
    font-size: 15px;
    font-weight: 600;
    letter-spacing: -0.012em;
    margin-bottom: 6px;
  }
  .line {
    font-size: 13px;
    color: var(--dim);
    text-wrap: pretty;
  }
</style>
