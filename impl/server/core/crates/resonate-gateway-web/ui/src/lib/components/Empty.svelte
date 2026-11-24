<script lang="ts">
  import { asset } from '$app/paths';
  /** Nothing to show, and why. A title and one explanatory line. */
  let { title, line }: { title: string; line: string } = $props();
</script>

<div class="empty">
  <img class="echo-mark" src={asset('images/resonate/echo.png')} width="55" height="55" alt="" />
  <div class="title">{title}</div>
  <div class="line">{line}</div>
</div>

<style>
  .empty {
    padding: 52px 24px;
    margin: 24px 26px;
    border: 1px solid var(--line);
    border-radius: 6px;
    background: var(--panel);
    text-align: center;
  }
  img {
    opacity: .65;
    margin-bottom: 18px;
  }
  .title {
    font-size: 20px;
    font-weight: 400;
    letter-spacing: -.5px;
    margin-bottom: 12px;
  }
  .line {
    font-size: 13px;
    color: var(--dim);
    text-wrap: pretty;
    max-width: 52ch;
    margin-inline: auto;
    line-height: 1.7;
  }
  @media (max-width: 767px) {
    .empty { margin: 16px 0; }
  }
</style>
