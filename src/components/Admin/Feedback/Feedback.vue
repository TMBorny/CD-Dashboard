<script setup lang="ts">
const feedbackFormUrl = 'https://docs.google.com/forms/d/e/1FAIpQLSd-5U6Z_bx-Z3wHyZnliiobRmZ-ORjtCaWHCjXKFoD77Fo8sw/viewform?embedded=true';
const feedbackFormExternalUrl = 'https://docs.google.com/forms/d/e/1FAIpQLSd-5U6Z_bx-Z3wHyZnliiobRmZ-ORjtCaWHCjXKFoD77Fo8sw/viewform?usp=publish-editor';
</script>

<template>
  <section class="min-h-screen bg-slate-50 px-4 py-6 sm:px-6 lg:px-8">
    <div class="mx-auto flex min-h-[calc(100vh-3rem)] max-w-6xl flex-col gap-6">
      <div class="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
        <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Feedback</p>
        <div class="mt-3 flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div class="max-w-2xl">
            <h1 class="text-3xl font-semibold tracking-tight text-slate-950">Share what would make the dashboard better</h1>
            <p class="mt-2 text-sm leading-6 text-slate-600">
              Use the embedded form below to send ideas, report issues, or suggest improvements without leaving the app.
            </p>
          </div>
          <a
            :href="feedbackFormExternalUrl"
            target="_blank"
            rel="noreferrer"
            class="inline-flex items-center justify-center rounded-full border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-slate-400 hover:bg-slate-50"
          >
            Open in Google Forms
          </a>
        </div>
      </div>

      <div class="min-h-0 flex-1 overflow-hidden rounded-[28px] border border-slate-200 bg-white shadow-sm">
        <iframe
          :src="feedbackFormUrl"
          title="Feedback form"
          class="h-[75vh] w-full border-0"
          loading="lazy"
          referrerpolicy="no-referrer-when-downgrade"
        />
      </div>
    </div>
  </section>
</template>
